"""
Objectify Worker
Dataset version -> Ontology instance bulk-create pipeline.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
from confluent_kafka import Consumer, KafkaError

from shared.config.service_config import ServiceConfig
from shared.models.objectify_job import ObjectifyJob
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyRegistry
from shared.services.lakefs_storage_service import create_lakefs_storage_service
from shared.services.lineage_store import LineageStore
from shared.services.sheet_import_service import FieldMapping, SheetImportService
from shared.utils.env_utils import parse_int_env
from shared.utils.s3_uri import parse_s3_uri
from shared.security.auth_utils import get_expected_token

logger = logging.getLogger(__name__)


class ObjectifyWorker:
    def __init__(self) -> None:
        self.running = False
        self.topic = (os.getenv("OBJECTIFY_JOBS_TOPIC") or "objectify-jobs").strip() or "objectify-jobs"
        self.group_id = (os.getenv("OBJECTIFY_JOBS_GROUP") or "objectify-worker-group").strip()
        self.consumer: Optional[Consumer] = None
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.objectify_registry: Optional[ObjectifyRegistry] = None
        self.lineage_store: Optional[LineageStore] = None
        self.storage = None
        self.http: Optional[httpx.AsyncClient] = None

        self.batch_size_default = parse_int_env("OBJECTIFY_BATCH_SIZE", 500, min_value=1, max_value=5000)
        self.max_rows_default = parse_int_env("OBJECTIFY_MAX_ROWS", 0, min_value=0, max_value=10_000_000)
        self.lineage_max_links = parse_int_env("OBJECTIFY_LINEAGE_MAX_LINKS", 1000, min_value=0, max_value=100_000)

    async def initialize(self) -> None:
        self.dataset_registry = DatasetRegistry()
        await self.dataset_registry.initialize()

        self.objectify_registry = ObjectifyRegistry()
        await self.objectify_registry.initialize()

        try:
            self.lineage_store = LineageStore()
            await self.lineage_store.initialize()
        except Exception as exc:
            logger.warning("LineageStore unavailable: %s", exc)
            self.lineage_store = None

        from shared.config.settings import ApplicationSettings

        self.storage = create_lakefs_storage_service(ApplicationSettings())
        if self.storage is None:
            raise RuntimeError("LakeFS storage service is required for objectify worker")

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        token = get_expected_token(("OMS_CLIENT_TOKEN", "OMS_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"))
        if token:
            headers["Authorization"] = f"Bearer {token}"
            headers["X-Admin-Token"] = token
        self.http = httpx.AsyncClient(base_url=ServiceConfig.get_oms_url(), timeout=60.0, headers=headers)

        self.consumer = Consumer(
            {
                "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 300000,
                "session.timeout.ms": 45000,
            }
        )
        self.consumer.subscribe([self.topic])

    async def close(self) -> None:
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.http:
            await self.http.aclose()
            self.http = None
        if self.lineage_store:
            await self.lineage_store.close()
            self.lineage_store = None
        if self.objectify_registry:
            await self.objectify_registry.close()
            self.objectify_registry = None
        if self.dataset_registry:
            await self.dataset_registry.close()
            self.dataset_registry = None

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        logger.info("ObjectifyWorker started (topic=%s)", self.topic)
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                payload = msg.value()
                if not payload:
                    continue
                try:
                    job = ObjectifyJob.model_validate_json(payload)
                    await self._process_job(job)
                    self.consumer.commit(message=msg, asynchronous=False)
                except Exception as exc:
                    logger.exception("Failed to process objectify job: %s", exc)
                    # Best-effort: commit to avoid poison pill loop.
                    self.consumer.commit(message=msg, asynchronous=False)
        finally:
            await self.close()

    async def _process_job(self, job: ObjectifyJob) -> None:
        if not self.objectify_registry:
            raise RuntimeError("ObjectifyRegistry not initialized")

        await self.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status="RUNNING",
        )

        mapping_spec = await self.objectify_registry.get_mapping_spec(mapping_spec_id=job.mapping_spec_id)
        if not mapping_spec:
            raise RuntimeError(f"Mapping spec not found: {job.mapping_spec_id}")
        if int(mapping_spec.version) != int(job.mapping_spec_version):
            raise RuntimeError(
                f"Mapping spec version mismatch (job={job.mapping_spec_version} spec={mapping_spec.version})"
            )

        columns, rows = await self._load_dataset_rows(job, mapping_spec.options or {})
        if not rows:
            raise RuntimeError("No rows loaded for objectify job")

        target_field_types = mapping_spec.target_field_types or {}
        if not target_field_types:
            target_field_types = await self._fetch_target_field_types(job)

        mappings = [
            FieldMapping(source_field=str(m.get("source_field") or ""), target_field=str(m.get("target_field") or ""))
            for m in mapping_spec.mappings
            if isinstance(m, dict)
        ]
        build = SheetImportService.build_instances(
            columns=columns,
            rows=rows,
            mappings=mappings,
            target_field_types=target_field_types,
            max_rows=job.max_rows,
        )
        errors = build.get("errors") or []

        instances = build.get("instances") or []
        instance_row_indices = build.get("instance_row_indices") or []
        error_row_indices = set(build.get("error_row_indices") or [])

        if errors and not job.allow_partial:
            await self.objectify_registry.update_objectify_job_status(
                job_id=job.job_id,
                status="FAILED",
                error="validation_failed",
                report={
                    "errors": errors[:200],
                    "stats": build.get("stats") or {},
                },
                completed_at=datetime.now(timezone.utc),
            )
            return

        if error_row_indices:
            filtered_instances = []
            filtered_indices = []
            for inst, row_idx in zip(instances, instance_row_indices):
                if row_idx in error_row_indices:
                    continue
                filtered_instances.append(inst)
                filtered_indices.append(row_idx)
            instances = filtered_instances
            instance_row_indices = filtered_indices

        if not instances:
            await self.objectify_registry.update_objectify_job_status(
                job_id=job.job_id,
                status="FAILED",
                error="no_valid_instances",
                report={
                    "errors": errors[:200],
                    "stats": build.get("stats") or {},
                },
                completed_at=datetime.now(timezone.utc),
            )
            return

        instances, instance_ids = self._ensure_instance_ids(
            instances,
            class_id=job.target_class_id,
            stable_seed=job.dataset_version_id,
        )

        batch_size = int(job.batch_size or self.batch_size_default)
        batch_size = max(1, min(batch_size, 5000))
        command_ids: List[str] = []
        for idx in range(0, len(instances), batch_size):
            batch = instances[idx : idx + batch_size]
            resp = await self._bulk_create_instances(job, batch)
            command_id = resp.get("command_id") if isinstance(resp, dict) else None
            if command_id:
                command_ids.append(str(command_id))

        await self._record_lineage(job, instance_ids)

        await self.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status="SUBMITTED",
            command_id=command_ids[0] if command_ids else None,
            report={
                "total_rows": len(rows),
                "prepared_instances": len(instances),
                "errors": errors[:200],
                "command_ids": command_ids,
                "instance_ids_sample": instance_ids[:10],
            },
            completed_at=datetime.now(timezone.utc),
        )

    async def _bulk_create_instances(self, job: ObjectifyJob, instances: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not self.http:
            raise RuntimeError("OMS client not initialized")
        branch = job.ontology_branch or job.dataset_branch or "main"
        resp = await self.http.post(
            f"/api/v1/instances/{job.db_name}/async/{job.target_class_id}/bulk-create",
            params={"branch": branch},
            json={
                "instances": instances,
                "metadata": {
                    "objectify_job_id": job.job_id,
                    "mapping_spec_id": job.mapping_spec_id,
                    "mapping_spec_version": job.mapping_spec_version,
                    "dataset_id": job.dataset_id,
                    "dataset_version_id": job.dataset_version_id,
                    "options": job.options,
                },
            },
        )
        resp.raise_for_status()
        if not resp.text:
            return {}
        return resp.json()

    async def _fetch_target_field_types(self, job: ObjectifyJob) -> Dict[str, str]:
        if not self.http:
            return {}
        branch = job.ontology_branch or job.dataset_branch or "main"
        resp = await self.http.get(
            f"/api/v1/database/{job.db_name}/ontology/{job.target_class_id}",
            params={"branch": branch},
        )
        resp.raise_for_status()
        payload = resp.json() if resp.text else {}
        data = payload.get("data") if isinstance(payload, dict) else {}
        properties = data.get("properties") if isinstance(data, dict) else []
        field_types: Dict[str, str] = {}
        if isinstance(properties, list):
            for prop in properties:
                if not isinstance(prop, dict):
                    continue
                name = str(prop.get("name") or "").strip()
                if not name:
                    continue
                dtype = prop.get("type") or prop.get("data_type") or prop.get("datatype")
                dtype = str(dtype).strip() if dtype is not None else "xsd:string"
                field_types[name] = dtype or "xsd:string"
        return field_types

    async def _load_dataset_rows(self, job: ObjectifyJob, options: Dict[str, Any]) -> Tuple[List[str], List[List[Any]]]:
        parsed = parse_s3_uri(job.artifact_key)
        if not parsed:
            raise ValueError(f"Invalid artifact_key: {job.artifact_key}")
        bucket, key = parsed
        has_header = options.get("source_has_header", True)
        delimiter = options.get("delimiter") or options.get("csv_delimiter") or ","
        max_rows = int(job.max_rows or self.max_rows_default)
        if max_rows <= 0:
            max_rows = None

        if key.endswith(".csv") or key.endswith(".tsv") or key.endswith(".txt"):
            raw = await self.storage.load_bytes(bucket, key)
            decoded = raw.decode("utf-8", errors="replace")
            reader = csv.reader(io.StringIO(decoded), delimiter=delimiter)
            rows: List[List[Any]] = []
            columns: List[str] = []
            for idx, row in enumerate(reader):
                if idx == 0 and has_header:
                    columns = [str(c).strip() for c in row]
                    continue
                if not columns:
                    columns = [f"col_{i}" for i in range(len(row))]
                rows.append(row)
                if max_rows and len(rows) >= max_rows:
                    break
            return columns, rows

        if key.endswith(".xlsx") or key.endswith(".xlsm") or key.endswith(".xls"):
            raise RuntimeError("Excel artifacts are not supported for objectify; convert via pipeline first.")

        # Treat as lakeFS prefix of JSON part files (Spark output)
        objects = await self.storage.list_objects(bucket, prefix=key)
        rows: List[List[Any]] = []
        columns: List[str] = []
        for obj in objects:
            obj_key = obj.get("Key")
            if not obj_key or obj_key.endswith("/"):
                continue
            if not obj_key.startswith(key):
                continue
            raw = await self.storage.load_bytes(bucket, obj_key)
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    if not columns:
                        columns = list(payload.keys())
                    rows.append([payload.get(col) for col in columns])
                elif isinstance(payload, list):
                    if not columns:
                        columns = [f"col_{i}" for i in range(len(payload))]
                    rows.append(payload)
                if max_rows and len(rows) >= max_rows:
                    break
            if max_rows and len(rows) >= max_rows:
                break
        return columns, rows

    def _ensure_instance_ids(
        self,
        instances: List[Dict[str, Any]],
        *,
        class_id: str,
        stable_seed: str,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        expected_key = f"{class_id.lower()}_id"
        instance_ids: List[str] = []
        for idx, inst in enumerate(instances):
            if not isinstance(inst, dict):
                continue
            candidate = inst.get(expected_key)
            if not candidate:
                for key, value in inst.items():
                    if key.endswith("_id") and value:
                        candidate = value
                        break
            if not candidate:
                suffix = uuid5(NAMESPACE_URL, f"objectify:{stable_seed}:{idx}").hex[:12]
                candidate = f"{class_id.lower()}_{suffix}"
                inst[expected_key] = candidate
            instance_ids.append(str(candidate))
        return instances, instance_ids

    async def _record_lineage(self, job: ObjectifyJob, instance_ids: List[str]) -> None:
        if not self.lineage_store:
            return
        limit = self.lineage_max_links
        if limit <= 0:
            return
        from_node_id = self.lineage_store.node_aggregate("DatasetVersion", job.dataset_version_id)
        for instance_id in instance_ids[:limit]:
            aggregate_id = f"{job.db_name}:{job.dataset_branch}:{job.target_class_id}:{instance_id}"
            to_node_id = self.lineage_store.node_aggregate("Instance", aggregate_id)
            try:
                await self.lineage_store.record_link(
                    from_node_id=from_node_id,
                    to_node_id=to_node_id,
                    edge_type="dataset_version_objectified",
                    occurred_at=datetime.now(timezone.utc),
                    db_name=job.db_name,
                    edge_metadata={
                        "db_name": job.db_name,
                        "dataset_id": job.dataset_id,
                        "dataset_version_id": job.dataset_version_id,
                        "mapping_spec_id": job.mapping_spec_id,
                        "target_class_id": job.target_class_id,
                    },
                )
            except Exception as exc:
                logger.warning("Failed to record lineage for instance %s: %s", instance_id, exc)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    worker = ObjectifyWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
