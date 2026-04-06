"""
Objectify write path strategies.

Encapsulates how objectified instances are persisted:
- Dataset-primary indexing (direct ES bulk index)
- instance-events S3 command files (for action writeback base state)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Protocol

from shared.config.instances_index_mapping import INSTANCE_INDEX_MAPPING
from shared.config.search_config import get_default_index_settings, get_instances_index_name
from shared.models.objectify_job import ObjectifyJob
from shared.services.core.instance_visibility import (
    OBJECTIFY_VISIBILITY_COMMITTED,
    OBJECTIFY_VISIBILITY_FIELD,
    OBJECTIFY_VISIBILITY_STAGED,
    apply_visible_instances_filter,
)
from shared.services.storage.elasticsearch_service import ElasticsearchService
from shared.utils.deterministic_ids import deterministic_uuid5_hex_prefix, deterministic_uuid5_str

if TYPE_CHECKING:
    from shared.services.storage.storage_service import StorageService

logger = logging.getLogger(__name__)


WRITE_PATH_MODE_DATASET_PRIMARY_INDEX = "dataset_primary_index"
OBJECTIFY_COMMAND_ID_SAMPLE_LIMIT = 25


@dataclass(frozen=True)
class ObjectifyWriteBatchResult:
    """Result for a single write batch.

    command_ids is intentionally a small sample for reporting/debugging.
    It is not the authoritative fan-out count.
    """

    command_ids: List[str]
    indexed_instance_ids: List[str]
    instance_event_files_written: int = 0
    instance_event_file_failures: int = 0
    instance_event_keys: List[str] = field(default_factory=list)


class ObjectifyWritePath(Protocol):
    """Port for objectify write-side strategies."""

    async def write_instances(
        self,
        *,
        job: ObjectifyJob,
        instances: List[Dict[str, Any]],
        ontology_version: Optional[Dict[str, str]],
        objectify_pk_fields: Optional[List[str]],
        objectify_instance_id_field: Optional[str],
        instance_relationships: Optional[Dict[str, Dict[str, Any]]] = None,
        target_field_types: Optional[Dict[str, str]] = None,
    ) -> ObjectifyWriteBatchResult:
        _ = (objectify_pk_fields, objectify_instance_id_field)
        raise NotImplementedError

    async def finalize_job(
        self,
        *,
        job: ObjectifyJob,
        execution_mode: str,
        indexed_instance_ids: Optional[Iterable[str]],
        deleted_instance_ids: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        ...

    async def list_job_instance_ids(
        self,
        *,
        job: ObjectifyJob,
    ) -> List[str]:
        ...

    async def cleanup_failed_job(
        self,
        *,
        job: ObjectifyJob,
        indexed_instance_ids: Optional[Iterable[str]] = None,
        instance_event_keys: Optional[Iterable[str]] = None,
    ) -> Dict[str, int]:
        ...


class DatasetPrimaryIndexWritePath:
    """
    Foundry-style path: dataset rows are indexed directly into Elasticsearch.

    The index remains a rebuildable read model; source-of-truth stays in dataset artifacts.
    """

    _INSTANCE_MAPPING: Dict[str, Any] = INSTANCE_INDEX_MAPPING

    def __init__(
        self,
        *,
        elasticsearch_service: ElasticsearchService,
        storage_service: Optional["StorageService"] = None,
        instance_bucket: str = "instance-events",
        chunk_size: int = 500,
        refresh: bool = False,
        prune_stale_on_full: bool = True,
    ) -> None:
        self._es = elasticsearch_service
        self._storage = storage_service
        self._instance_bucket = instance_bucket
        self._chunk_size = max(1, int(chunk_size))
        self._refresh = bool(refresh)
        self._prune_stale_on_full = bool(prune_stale_on_full)
        self._created_indices: set[str] = set()

    async def write_instances(
        self,
        *,
        job: ObjectifyJob,
        instances: List[Dict[str, Any]],
        ontology_version: Optional[Dict[str, str]],
        objectify_pk_fields: Optional[List[str]],
        objectify_instance_id_field: Optional[str],
        instance_relationships: Optional[Dict[str, Dict[str, Any]]] = None,
        target_field_types: Optional[Dict[str, str]] = None,
    ) -> ObjectifyWriteBatchResult:
        _ = (objectify_pk_fields, objectify_instance_id_field)
        if not instances:
            return ObjectifyWriteBatchResult(
                command_ids=[],
                indexed_instance_ids=[],
                instance_event_files_written=0,
                instance_event_file_failures=0,
            )

        branch = job.ontology_branch or job.dataset_branch or "main"
        index_name = await self._ensure_instances_index(db_name=job.db_name, branch=branch)
        now = datetime.now(timezone.utc).isoformat()
        batch_sequence = int(datetime.now(timezone.utc).timestamp() * 1000)

        docs: List[Dict[str, Any]] = []
        indexed_instance_ids: List[str] = []
        rels_lookup = instance_relationships or {}

        for inst in instances:
            if not isinstance(inst, dict):
                continue
            raw_instance_id = inst.get("instance_id")
            if raw_instance_id is None:
                raise ValueError("instance_id is required for dataset-primary indexing")
            instance_id = str(raw_instance_id).strip()
            if not instance_id:
                raise ValueError("instance_id cannot be blank for dataset-primary indexing")

            indexed_instance_ids.append(instance_id)
            docs.append(
                {
                    "_id": instance_id,
                    **self._build_document(
                        job=job,
                        instance=inst,
                        instance_id=instance_id,
                        branch=branch,
                        ontology_version=ontology_version,
                        now_iso=now,
                        event_sequence=batch_sequence,
                        relationships=rels_lookup.get(instance_id),
                        target_field_types=target_field_types,
                    ),
                }
            )

        if docs:
            result = await self._es.bulk_index(
                index=index_name,
                documents=docs,
                chunk_size=self._chunk_size,
                refresh=self._refresh,
            )
            failed = int(result.get("failed") or 0)
            if failed > 0:
                raise RuntimeError(f"dataset_primary_bulk_index_failed:{failed}")

        return ObjectifyWriteBatchResult(
            command_ids=[],
            indexed_instance_ids=indexed_instance_ids,
            instance_event_files_written=0,
            instance_event_file_failures=0,
            instance_event_keys=[],
        )

    async def _write_instance_commands_to_s3(
        self,
        *,
        job: ObjectifyJob,
        index_name: str,
        indexed_instance_ids: Iterable[str],
        branch: str,
    ) -> Dict[str, Any]:
        """Publish BULK_CREATE_INSTANCES command files after authoritative job commit."""
        if not self._storage:
            return {
                "written": 0,
                "failed": 0,
                "command_ids": [],
                "command_ids_truncated": False,
                "keys": [],
                "reason": "storage_unavailable_or_not_configured",
            }

        requested_ids = [str(value).strip() for value in indexed_instance_ids if str(value).strip()]
        if not requested_ids:
            return {
                "written": 0,
                "failed": 0,
                "command_ids": [],
                "command_ids_truncated": False,
                "keys": [],
                "reason": "no_instances_indexed",
            }

        documents = await self._load_instance_documents(
            index_name=index_name,
            instance_ids=requested_ids,
        )
        now_iso = datetime.now(timezone.utc).isoformat()

        written = 0
        failed_count = 0
        command_ids_sample: List[str] = []
        written_keys: List[str] = []
        for instance_id in requested_ids:
            source_doc = documents.get(instance_id)
            if not isinstance(source_doc, Mapping):
                failed_count += 1
                if failed_count <= 3:
                    logger.warning(
                        "Missing staged objectify document during command publish (job=%s instance=%s)",
                        job.job_id,
                        instance_id,
                    )
                continue

            raw_payload = source_doc.get("data") if isinstance(source_doc.get("data"), Mapping) else {}
            payload: Dict[str, Any] = {}
            _skip = {"instance_id", "class_id", "db_name", "branch", "properties", "_metadata"}
            for key, value in raw_payload.items():
                if key not in _skip:
                    payload[key] = value

            command_id = str(source_doc.get("event_id") or "").strip()
            if not command_id:
                event_sequence = str(source_doc.get("event_sequence") or "0").strip()
                command_id = deterministic_uuid5_str(
                    f"objectify:{job.job_id}:{instance_id}:{event_sequence}"
                )
            created_at = str(
                source_doc.get("created_at")
                or source_doc.get("event_timestamp")
                or now_iso
            )
            key_timestamp = created_at.replace(":", "").replace("-", "").replace("+", "")
            resolved_branch = str(source_doc.get("branch") or branch or "main").strip() or "main"

            command_file: Dict[str, Any] = {
                "command_type": "BULK_CREATE_INSTANCES",
                "command_id": command_id,
                "instance_id": instance_id,
                "class_id": job.target_class_id,
                "db_name": job.db_name,
                "branch": resolved_branch,
                "payload": payload,
                "created_at": created_at,
                "created_by": f"objectify:{job.job_id}",
            }

            s3_key = (
                f"{job.db_name}/{resolved_branch}/{job.target_class_id}/{instance_id}/"
                f"{key_timestamp}_{command_id}_BULK_CREATE_INSTANCES.json"
            )
            try:
                await self._storage.save_json(
                    self._instance_bucket,
                    s3_key,
                    command_file,
                )
                written += 1
                written_keys.append(s3_key)
                if len(command_ids_sample) < OBJECTIFY_COMMAND_ID_SAMPLE_LIMIT:
                    command_ids_sample.append(command_id)
            except Exception as exc:
                failed_count += 1
                if failed_count <= 3:
                    logger.warning(
                        "Failed to write instance command to S3 (instance=%s): %s",
                        instance_id, exc,
                    )

        if written:
            logger.info(
                "Wrote %d instance command files to s3://%s (class=%s, job=%s, failed=%d)",
                written, self._instance_bucket, job.target_class_id, job.job_id, failed_count,
            )
        return {
            "written": written,
            "failed": failed_count,
            "command_ids": command_ids_sample,
            "command_ids_truncated": written > len(command_ids_sample),
            "keys": written_keys,
        }

    async def _load_instance_documents(
        self,
        *,
        index_name: str,
        instance_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        documents: Dict[str, Dict[str, Any]] = {}
        for idx in range(0, len(instance_ids), self._chunk_size):
            chunk = instance_ids[idx : idx + self._chunk_size]
            response = await self._es.client.mget(index=index_name, body={"ids": chunk})
            payload = getattr(response, "body", response)
            docs = payload.get("docs") if isinstance(payload, Mapping) else None
            if not isinstance(docs, list):
                continue
            for item in docs:
                if not isinstance(item, Mapping) or not item.get("found"):
                    continue
                source = item.get("_source") if isinstance(item.get("_source"), Mapping) else None
                if not isinstance(source, Mapping):
                    continue
                instance_id = str(item.get("_id") or source.get("instance_id") or "").strip()
                if instance_id:
                    documents[instance_id] = dict(source)
        return documents

    async def finalize_job(
        self,
        *,
        job: ObjectifyJob,
        execution_mode: str,
        indexed_instance_ids: Optional[Iterable[str]],
        deleted_instance_ids: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        branch = job.ontology_branch or job.dataset_branch or "main"
        index_name = await self._ensure_instances_index(db_name=job.db_name, branch=branch)
        active_ids = {
            str(v).strip()
            for v in (
                indexed_instance_ids
                if indexed_instance_ids is not None
                else await self.list_job_instance_ids(job=job)
            )
            if str(v).strip()
        }
        pending_deleted_ids = {
            str(v).strip()
            for v in (deleted_instance_ids or [])
            if str(v).strip() and str(v).strip() not in active_ids
        }
        instance_event_summary = await self._write_instance_commands_to_s3(
            job=job,
            index_name=index_name,
            indexed_instance_ids=sorted(active_ids),
            branch=branch,
        )
        publish_summary = await self._publish_job_visibility(
            index_name=index_name,
            instance_ids=active_ids,
        )
        published_count = int(publish_summary.get("published") or 0)
        if active_ids and published_count != len(active_ids):
            raise RuntimeError(
                "objectify_visibility_publish_incomplete:"
                f"expected={len(active_ids)} published={published_count}"
            )
        explicit_delete_summary = await self._delete_explicit_instance_ids(
            index_name=index_name,
            instance_ids=pending_deleted_ids,
        )

        resolved_mode = str(execution_mode or "").strip().lower()
        prune_reason: Optional[str] = None
        if resolved_mode != "full":
            prune_reason = "execution_mode_not_full"
        elif not self._prune_stale_on_full:
            prune_reason = "prune_disabled"
        elif job.max_rows is not None:
            prune_reason = "max_rows_set"

        if prune_reason is not None:
            await self._es.refresh_index(index_name)
            return {
                "instance_event_files": instance_event_summary,
                "visibility_publish": publish_summary,
                "explicit_delete": explicit_delete_summary,
                "stale_prune": {
                    "enabled": bool(self._prune_stale_on_full),
                    "executed": False,
                    "deleted": 0,
                    "reason": prune_reason,
                }
            }

        stale_ids = await self._find_stale_instance_ids(
            index_name=index_name,
            class_id=job.target_class_id,
            active_instance_ids=active_ids,
        )

        deleted = 0
        for stale_id in stale_ids:
            ok = await self._es.delete_document(index=index_name, doc_id=stale_id, refresh=False)
            if ok:
                deleted += 1

        await self._es.refresh_index(index_name)

        return {
            "instance_event_files": instance_event_summary,
            "visibility_publish": publish_summary,
            "explicit_delete": explicit_delete_summary,
            "stale_prune": {
                "enabled": bool(self._prune_stale_on_full),
                "executed": True,
                "deleted": deleted,
                "candidates": len(stale_ids),
            }
        }

    async def _delete_explicit_instance_ids(
        self,
        *,
        index_name: str,
        instance_ids: set[str],
    ) -> Dict[str, Any]:
        if not instance_ids:
            return {
                "executed": False,
                "deleted": 0,
                "reason": "no_explicit_deletes",
            }

        deleted = 0
        for instance_id in sorted(instance_ids):
            if await self._es.delete_document(index=index_name, doc_id=instance_id, refresh=False):
                deleted += 1
        return {
            "executed": True,
            "deleted": deleted,
            "requested": len(instance_ids),
        }

    async def list_job_instance_ids(
        self,
        *,
        job: ObjectifyJob,
    ) -> List[str]:
        branch = job.ontology_branch or job.dataset_branch or "main"
        index_name = await self._ensure_instances_index(db_name=job.db_name, branch=branch)
        instance_ids: List[str] = []
        search_after: Optional[List[Any]] = None
        page_size = min(max(self._chunk_size, 200), 2000)
        query: Dict[str, Any] = {
            "bool": {
                "filter": [
                    {"term": {"class_id": job.target_class_id}},
                    {"term": {"backing_dataset.objectify_job_id": job.job_id}},
                ]
            }
        }

        while True:
            body: Dict[str, Any] = {
                "query": query,
                "_source": ["instance_id"],
                "size": page_size,
                "sort": [{"instance_id": {"order": "asc"}}],
                "track_total_hits": False,
            }
            if search_after:
                body["search_after"] = search_after

            response = await self._es.client.search(index=index_name, body=body)
            payload = getattr(response, "body", response)
            hits_outer = payload.get("hits") if isinstance(payload, Mapping) else None
            hits = hits_outer.get("hits") if isinstance(hits_outer, Mapping) else None
            if not isinstance(hits, list) or not hits:
                break

            for hit in hits:
                if not isinstance(hit, Mapping):
                    continue
                source = hit.get("_source") if isinstance(hit.get("_source"), Mapping) else {}
                instance_id = str(source.get("instance_id") or hit.get("_id") or "").strip()
                if instance_id:
                    instance_ids.append(instance_id)

            last = hits[-1] if isinstance(hits[-1], Mapping) else {}
            raw_sort = last.get("sort") if isinstance(last, Mapping) else None
            if not isinstance(raw_sort, list) or not raw_sort:
                break
            search_after = raw_sort

        return instance_ids

    async def cleanup_failed_job(
        self,
        *,
        job: ObjectifyJob,
        indexed_instance_ids: Optional[Iterable[str]] = None,
        instance_event_keys: Optional[Iterable[str]] = None,
    ) -> Dict[str, int]:
        branch = job.ontology_branch or job.dataset_branch or "main"
        index_name = await self._ensure_instances_index(db_name=job.db_name, branch=branch)
        deleted_docs = 0
        deleted_objects = 0
        active_ids = {
            str(value).strip()
            for value in (
                indexed_instance_ids
                if indexed_instance_ids is not None
                else await self.list_job_instance_ids(job=job)
            )
            if str(value).strip()
        }

        for instance_id in active_ids:
            if await self._es.delete_document(index=index_name, doc_id=instance_id, refresh=False):
                deleted_docs += 1
        if deleted_docs:
            await self._es.refresh_index(index_name)

        if self._storage:
            for key in instance_event_keys or []:
                key_token = str(key or "").strip()
                if not key_token:
                    continue
                try:
                    if await self._storage.delete_object(self._instance_bucket, key_token):
                        deleted_objects += 1
                except Exception as exc:
                    logger.warning(
                        "Failed to delete objectify instance-event file during cleanup (job=%s key=%s): %s",
                        job.job_id,
                        key_token,
                        exc,
                    )

        return {
            "deleted_docs": deleted_docs,
            "deleted_instance_event_files": deleted_objects,
        }

    async def _ensure_instances_index(self, *, db_name: str, branch: str) -> str:
        index_name = get_instances_index_name(db_name, branch=branch)

        if index_name in self._created_indices:
            try:
                if await self._es.index_exists(index_name):
                    return index_name
            except Exception as exc:
                logger.warning(
                    "Failed to verify cached instances index existence (%s): %s",
                    index_name,
                    exc,
                    exc_info=True,
                )
            self._created_indices.discard(index_name)

        if not await self._es.index_exists(index_name):
            await self._es.create_index(
                index=index_name,
                mappings=self._INSTANCE_MAPPING,
                settings=get_default_index_settings(),
            )

        # Always reconcile mapping after create attempt to close
        # index-exists/create race windows.
        await self._es.update_mapping(index=index_name, properties=self._INSTANCE_MAPPING["properties"])
        logger.info("Ensured dataset-primary instances index + mapping: %s", index_name)

        self._created_indices.add(index_name)
        return index_name

    async def _find_stale_instance_ids(
        self,
        *,
        index_name: str,
        class_id: str,
        active_instance_ids: set[str],
    ) -> List[str]:
        stale_ids: List[str] = []
        search_after: Optional[List[Any]] = None
        page_size = min(max(self._chunk_size, 200), 2000)

        while True:
            body: Dict[str, Any] = {
                "query": apply_visible_instances_filter({"term": {"class_id": class_id}}),
                "_source": ["instance_id"],
                "size": page_size,
                "sort": [{"instance_id": {"order": "asc"}}],
                "track_total_hits": False,
            }
            if search_after:
                body["search_after"] = search_after

            response = await self._es.client.search(index=index_name, body=body)
            payload = getattr(response, "body", response)
            if not isinstance(payload, Mapping):
                break
            hits_outer = payload.get("hits")
            if not isinstance(hits_outer, Mapping):
                break
            hits = hits_outer.get("hits")
            if not isinstance(hits, list) or not hits:
                break

            for hit in hits:
                if not isinstance(hit, Mapping):
                    continue
                source = hit.get("_source")
                if not isinstance(source, Mapping):
                    source = {}
                raw_id = source.get("instance_id") or hit.get("_id")
                if raw_id is None:
                    continue
                instance_id = str(raw_id).strip()
                if not instance_id:
                    continue
                if instance_id not in active_instance_ids:
                    stale_ids.append(instance_id)

            last = hits[-1] if isinstance(hits[-1], Mapping) else {}
            raw_sort = last.get("sort") if isinstance(last, Mapping) else None
            if not isinstance(raw_sort, list) or not raw_sort:
                break
            search_after = raw_sort

        return stale_ids

    async def _publish_job_visibility(
        self,
        *,
        index_name: str,
        instance_ids: set[str],
    ) -> Dict[str, Any]:
        if not instance_ids:
            return {
                "executed": False,
                "published": 0,
                "reason": "no_instances_indexed",
                "refresh": True,
            }

        published = 0
        sorted_ids = sorted(instance_ids)
        chunk_size = min(max(self._chunk_size, 50), 500)
        for offset in range(0, len(sorted_ids), chunk_size):
            batch = sorted_ids[offset : offset + chunk_size]
            results = await asyncio.gather(
                *[
                    self._es.update_document(
                        index=index_name,
                        doc_id=instance_id,
                        doc={OBJECTIFY_VISIBILITY_FIELD: OBJECTIFY_VISIBILITY_COMMITTED},
                        refresh=False,
                    )
                    for instance_id in batch
                ]
            )
            published += sum(1 for item in results if item)

        return {
            "executed": True,
            "published": published,
            "refresh": True,
        }

    @staticmethod
    def _build_document(
        *,
        job: ObjectifyJob,
        instance: Dict[str, Any],
        instance_id: str,
        branch: str,
        ontology_version: Optional[Dict[str, str]],
        now_iso: str,
        event_sequence: int,
        relationships: Optional[Dict[str, Any]] = None,
        target_field_types: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        payload = dict(instance)
        payload.setdefault("instance_id", instance_id)
        payload.setdefault("class_id", job.target_class_id)
        payload.setdefault("db_name", job.db_name)
        payload.setdefault("branch", branch)

        ontology_ref = None
        ontology_commit = None
        if isinstance(ontology_version, Mapping):
            ontology_ref = ontology_version.get("ref")
            ontology_commit = ontology_version.get("commit")

        lifecycle_seed = f"{job.db_name}:{job.target_class_id}:{instance_id}"
        lifecycle_id = f"lc-{deterministic_uuid5_hex_prefix(lifecycle_seed, length=12)}"
        event_id = deterministic_uuid5_str(f"objectify:{job.job_id}:{instance_id}:{event_sequence}")

        properties = payload.get("properties")
        if not isinstance(properties, list) or not properties:
            # Auto-build properties nested array from flat instance fields
            # so ES nested queries (filtering) work on indexed data.
            properties = []
            _skip = {"instance_id", "class_id", "db_name", "branch", "properties"}
            _field_types = target_field_types or {}
            for key, value in payload.items():
                if key in _skip or value is None:
                    continue
                str_value = str(value).strip()
                if not str_value:
                    continue
                prop: Dict[str, Any] = {"name": key, "value": str_value}
                ftype = _field_types.get(key)
                if ftype:
                    prop["type"] = ftype
                properties.append(prop)

        backing_dataset: Dict[str, Any] = {
            "dataset_id": job.dataset_id,
            "dataset_version_id": job.dataset_version_id,
            "artifact_id": job.artifact_id,
            "mapping_spec_id": job.mapping_spec_id,
            "mapping_spec_version": int(job.mapping_spec_version) if job.mapping_spec_version is not None else 0,
            "objectify_job_id": job.job_id,
            "indexed_at": now_iso,
        }

        return {
            "instance_id": instance_id,
            "class_id": job.target_class_id,
            "class_label": job.target_class_id,
            "properties": properties,
            "data": payload,
            "relationships": relationships or {},
            "backing_dataset": backing_dataset,
            OBJECTIFY_VISIBILITY_FIELD: OBJECTIFY_VISIBILITY_STAGED,
            "lifecycle_id": lifecycle_id,
            "event_id": event_id,
            "event_sequence": int(event_sequence),
            "event_timestamp": now_iso,
            "version": int(job.mapping_spec_version) if job.mapping_spec_version is not None else 0,
            "db_name": job.db_name,
            "branch": branch,
            "ontology_ref": str(ontology_ref).strip() if ontology_ref else None,
            "ontology_commit": str(ontology_commit).strip() if ontology_commit else None,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
