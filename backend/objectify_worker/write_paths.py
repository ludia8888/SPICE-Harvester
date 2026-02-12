"""
Objectify write path strategies.

Encapsulates how objectified instances are persisted:
- Dataset-primary indexing (direct ES bulk index)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol

from shared.config.search_config import get_default_index_settings, get_instances_index_name
from shared.models.objectify_job import ObjectifyJob
from shared.services.storage.elasticsearch_service import ElasticsearchService
from shared.utils.deterministic_ids import deterministic_uuid5_hex_prefix, deterministic_uuid5_str

logger = logging.getLogger(__name__)


WRITE_PATH_MODE_DATASET_PRIMARY_INDEX = "dataset_primary_index"


@dataclass(frozen=True)
class ObjectifyWriteBatchResult:
    """Result for a single write batch."""

    command_ids: List[str]
    indexed_instance_ids: List[str]


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
        ...

    async def finalize_job(
        self,
        *,
        job: ObjectifyJob,
        execution_mode: str,
        indexed_instance_ids: Iterable[str],
    ) -> Dict[str, Any]:
        ...


class DatasetPrimaryIndexWritePath:
    """
    Foundry-style path: dataset rows are indexed directly into Elasticsearch.

    The index remains a rebuildable read model; source-of-truth stays in dataset artifacts.
    """

    _INSTANCE_MAPPING: Dict[str, Any] = {
        "properties": {
            "instance_id": {"type": "keyword"},
            "class_id": {"type": "keyword"},
            "class_label": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "properties": {
                "type": "nested",
                "properties": {
                    "name": {"type": "keyword"},
                    "value": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "type": {"type": "keyword"},
                },
            },
            "data": {"enabled": False},
            "lifecycle_id": {"type": "keyword"},
            "event_id": {"type": "keyword"},
            "event_sequence": {"type": "long"},
            "event_timestamp": {"type": "date"},
            "version": {"type": "long"},
            "db_name": {"type": "keyword"},
            "branch": {"type": "keyword"},
            "ontology_ref": {"type": "keyword"},
            "ontology_commit": {"type": "keyword"},
            "created_at": {"type": "date"},
            "updated_at": {"type": "date"},
            "overlay_tombstone": {"type": "boolean"},
            "patchset_commit_id": {"type": "keyword"},
            "action_log_id": {"type": "keyword"},
            "conflict_status": {"type": "keyword"},
            "base_token": {"type": "object", "enabled": True},
            "relationships": {"type": "object", "enabled": True},
            "backing_dataset": {
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "keyword"},
                    "dataset_version_id": {"type": "keyword"},
                    "artifact_id": {"type": "keyword"},
                    "mapping_spec_id": {"type": "keyword"},
                    "mapping_spec_version": {"type": "long"},
                    "objectify_job_id": {"type": "keyword"},
                    "indexed_at": {"type": "date"},
                },
            },
        }
    }

    def __init__(
        self,
        *,
        elasticsearch_service: ElasticsearchService,
        chunk_size: int = 500,
        refresh: bool = False,
        prune_stale_on_full: bool = True,
    ) -> None:
        self._es = elasticsearch_service
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
        if not instances:
            return ObjectifyWriteBatchResult(command_ids=[], indexed_instance_ids=[])

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

        return ObjectifyWriteBatchResult(command_ids=[], indexed_instance_ids=indexed_instance_ids)

    async def finalize_job(
        self,
        *,
        job: ObjectifyJob,
        execution_mode: str,
        indexed_instance_ids: Iterable[str],
    ) -> Dict[str, Any]:
        if str(execution_mode or "").strip().lower() != "full" or not self._prune_stale_on_full:
            return {
                "stale_prune": {
                    "enabled": bool(self._prune_stale_on_full),
                    "executed": False,
                    "deleted": 0,
                }
            }

        branch = job.ontology_branch or job.dataset_branch or "main"
        index_name = await self._ensure_instances_index(db_name=job.db_name, branch=branch)
        active_ids = {str(v).strip() for v in indexed_instance_ids if str(v).strip()}
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

        if self._refresh and stale_ids:
            await self._es.refresh_index(index_name)

        return {
            "stale_prune": {
                "enabled": bool(self._prune_stale_on_full),
                "executed": True,
                "deleted": deleted,
                "candidates": len(stale_ids),
            }
        }

    async def _ensure_instances_index(self, *, db_name: str, branch: str) -> str:
        index_name = get_instances_index_name(db_name, branch=branch)

        if index_name in self._created_indices:
            try:
                if await self._es.index_exists(index_name):
                    return index_name
            except Exception:
                pass
            self._created_indices.discard(index_name)

        if not await self._es.index_exists(index_name):
            await self._es.create_index(
                index=index_name,
                mappings=self._INSTANCE_MAPPING,
                settings=get_default_index_settings(),
            )
            logger.info("Created dataset-primary instances index: %s", index_name)
        else:
            # Ensure required fields exist for forward compatibility.
            await self._es.update_mapping(index=index_name, properties=self._INSTANCE_MAPPING["properties"])

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
                "query": {"term": {"class_id": class_id}},
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
