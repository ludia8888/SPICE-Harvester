"""
Objectify Worker
Dataset version -> Ontology instance bulk-create pipeline.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import queue as queue_module
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import NAMESPACE_URL, uuid5

import httpx
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from shared.config.service_config import ServiceConfig
from shared.models.objectify_job import ObjectifyJob
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyRegistry
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.lakefs_storage_service import create_lakefs_storage_service
from shared.services.lineage_store import LineageStore
from shared.services.processed_event_registry import ClaimDecision, ProcessedEventRegistry
from shared.services.sheet_import_service import FieldMapping, SheetImportService
from shared.utils.env_utils import parse_int_env
from shared.utils.s3_uri import parse_s3_uri
from shared.errors.enterprise_catalog import resolve_objectify_error
from shared.security.auth_utils import get_expected_token

logger = logging.getLogger(__name__)


class ObjectifyNonRetryableError(RuntimeError):
    """Raised for objectify failures that should not be retried."""


class ObjectifyWorker:
    def __init__(self) -> None:
        self.running = False
        self.topic = (os.getenv("OBJECTIFY_JOBS_TOPIC") or "objectify-jobs").strip() or "objectify-jobs"
        self.dlq_topic = (
            (os.getenv("OBJECTIFY_JOBS_DLQ_TOPIC") or "objectify-jobs-dlq").strip() or "objectify-jobs-dlq"
        )
        self.group_id = (os.getenv("OBJECTIFY_JOBS_GROUP") or "objectify-worker-group").strip()
        self.handler = (os.getenv("OBJECTIFY_WORKER_HANDLER") or "objectify_worker").strip()
        self.consumer: Optional[Consumer] = None
        self.dlq_producer: Optional[Producer] = None
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.objectify_registry: Optional[ObjectifyRegistry] = None
        self.pipeline_registry: Optional[PipelineRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.lineage_store: Optional[LineageStore] = None
        self.storage = None
        self.http: Optional[httpx.AsyncClient] = None

        self.batch_size_default = parse_int_env("OBJECTIFY_BATCH_SIZE", 500, min_value=1, max_value=5000)
        self.row_batch_size_default = parse_int_env("OBJECTIFY_ROW_BATCH_SIZE", 1000, min_value=1, max_value=50000)
        self.list_page_size = parse_int_env("OBJECTIFY_LIST_PAGE_SIZE", 1000, min_value=10, max_value=10000)
        self.max_rows_default = parse_int_env("OBJECTIFY_MAX_ROWS", 0, min_value=0, max_value=10_000_000)
        self.lineage_max_links = parse_int_env("OBJECTIFY_LINEAGE_MAX_LINKS", 1000, min_value=0, max_value=100_000)
        self.max_retries = parse_int_env("OBJECTIFY_MAX_RETRIES", 5, min_value=1, max_value=100)
        self.backoff_base = parse_int_env("OBJECTIFY_BACKOFF_BASE_SECONDS", 2, min_value=0, max_value=300)
        self.backoff_max = parse_int_env("OBJECTIFY_BACKOFF_MAX_SECONDS", 60, min_value=1, max_value=3600)

    async def initialize(self) -> None:
        self.dataset_registry = DatasetRegistry()
        await self.dataset_registry.initialize()

        self.objectify_registry = ObjectifyRegistry()
        await self.objectify_registry.initialize()

        self.pipeline_registry = PipelineRegistry()
        await self.pipeline_registry.initialize()

        self.processed = ProcessedEventRegistry()
        await self.processed.initialize()

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

        self.dlq_producer = Producer(
            {
                "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                "client.id": os.getenv("SERVICE_NAME") or "objectify-worker",
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 100,
                "linger.ms": 20,
                "compression.type": "snappy",
            }
        )

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
        if self.processed:
            await self.processed.close()
            self.processed = None
        if self.objectify_registry:
            await self.objectify_registry.close()
            self.objectify_registry = None
        if self.pipeline_registry:
            await self.pipeline_registry.close()
            self.pipeline_registry = None
        if self.dataset_registry:
            await self.dataset_registry.close()
            self.dataset_registry = None
        if self.dlq_producer:
            try:
                self.dlq_producer.flush(5)
            except Exception:
                pass
            self.dlq_producer = None

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
                raw_text: Optional[str] = None
                job: Optional[ObjectifyJob] = None
                try:
                    raw_text = payload.decode("utf-8", errors="replace") if isinstance(payload, (bytes, bytearray)) else None
                    job = ObjectifyJob.model_validate_json(payload)
                except Exception as exc:
                    logger.exception("Invalid objectify payload; skipping: %s", exc)
                    self.consumer.commit(message=msg, asynchronous=False)
                    continue

                claim = None
                heartbeat_task: Optional[asyncio.Task] = None
                try:
                    if not self.processed:
                        raise RuntimeError("ProcessedEventRegistry not initialized")

                    claim = await self.processed.claim(
                        handler=self.handler,
                        event_id=job.job_id,
                        aggregate_id=job.dataset_id,
                    )
                    if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
                        self.consumer.commit(message=msg, asynchronous=False)
                        continue
                    if claim.decision == ClaimDecision.IN_PROGRESS:
                        await asyncio.sleep(2)
                        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                        continue

                    heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(handler=self.handler, event_id=job.job_id)
                    )

                    await self._process_job(job)
                    if self.processed:
                        await self.processed.mark_done(handler=self.handler, event_id=job.job_id)
                    self.consumer.commit(message=msg, asynchronous=False)
                except Exception as exc:
                    err = str(exc)
                    retryable = self._is_retryable_error(exc)
                    attempt_count = int(getattr(claim, "attempt_count", 1) or 1)

                    if self.objectify_registry and job:
                        try:
                            report_payload = {"error": err}
                            enterprise = resolve_objectify_error(err)
                            if enterprise:
                                report_payload["enterprise"] = enterprise.to_dict()
                            await self.objectify_registry.update_objectify_job_status(
                                job_id=job.job_id,
                                status="FAILED",
                                error=err[:4000],
                                report=report_payload,
                                completed_at=datetime.now(timezone.utc),
                            )
                        except Exception:
                            pass

                    if self.processed and job:
                        try:
                            await self.processed.mark_failed(handler=self.handler, event_id=job.job_id, error=err)
                        except Exception as mark_err:
                            logger.warning("Failed to mark objectify job failed: %s", mark_err)

                    if not retryable:
                        attempt_count = self.max_retries

                    if attempt_count >= self.max_retries:
                        logger.error(
                            "Objectify job max retries exceeded; sending to DLQ (job_id=%s attempt=%s)",
                            job.job_id if job else None,
                            attempt_count,
                        )
                        await self._send_to_dlq(
                            job=job,
                            raw_payload=raw_text,
                            error=err,
                            attempt_count=attempt_count,
                        )
                        self.consumer.commit(message=msg, asynchronous=False)
                        continue

                    backoff_s = min(self.backoff_max, int(self.backoff_base * (2 ** max(0, attempt_count - 1))))
                    logger.warning(
                        "Objectify job failed; will retry (job_id=%s attempt=%s backoff=%ss): %s",
                        job.job_id if job else None,
                        attempt_count,
                        backoff_s,
                        err,
                    )
                    await asyncio.sleep(backoff_s)
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                finally:
                    if heartbeat_task:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass
        finally:
            await self.close()

    async def _process_job(self, job: ObjectifyJob) -> None:
        if not self.objectify_registry or not self.dataset_registry:
            raise RuntimeError("ObjectifyRegistry not initialized")

        async def _fail_job(error: str, *, report: Optional[Dict[str, Any]] = None) -> None:
            report_payload = dict(report or {})
            report_payload.setdefault("error", error)
            enterprise = resolve_objectify_error(error)
            if enterprise:
                report_payload.setdefault("enterprise", enterprise.to_dict())
            await self.objectify_registry.update_objectify_job_status(
                job_id=job.job_id,
                status="FAILED",
                error=error[:4000],
                report=report_payload,
                completed_at=datetime.now(timezone.utc),
            )
            raise ObjectifyNonRetryableError(error)

        record = await self.objectify_registry.get_objectify_job(job_id=job.job_id)
        if record and record.status in {"SUBMITTED", "COMPLETED"}:
            logger.info("Objectify job already submitted (job_id=%s); skipping", job.job_id)
            return

        dataset = await self.dataset_registry.get_dataset(dataset_id=job.dataset_id)
        if not dataset:
            await _fail_job(f"dataset_not_found:{job.dataset_id}")
        if dataset.db_name != job.db_name:
            await _fail_job("dataset_db_name_mismatch")

        input_type = "dataset_version" if job.dataset_version_id else "artifact"
        resolved_output_name: Optional[str] = None
        stable_seed: Optional[str] = None

        if job.dataset_version_id and job.artifact_id:
            await _fail_job("objectify_input_conflict")
        if not job.dataset_version_id and not job.artifact_id:
            await _fail_job("objectify_input_missing")

        if job.dataset_version_id:
            version = await self.dataset_registry.get_version(version_id=job.dataset_version_id)
            if not version or version.dataset_id != job.dataset_id:
                await _fail_job("dataset_version_mismatch")
            resolved_artifact_key = job.artifact_key or version.artifact_key
            if not resolved_artifact_key:
                await _fail_job("artifact_key_missing")
            if version.artifact_key and job.artifact_key and version.artifact_key != job.artifact_key:
                await _fail_job("artifact_key_mismatch")
            stable_seed = job.dataset_version_id
        else:
            resolved_artifact_key, resolved_output_name = await self._resolve_artifact_output(job)
            if job.artifact_key and job.artifact_key != resolved_artifact_key:
                await _fail_job("artifact_key_mismatch")
            stable_seed = f"artifact:{job.artifact_id}:{resolved_output_name}"

        job.artifact_key = resolved_artifact_key
        if resolved_output_name and not job.artifact_output_name:
            job.artifact_output_name = resolved_output_name

        mapping_spec = await self.objectify_registry.get_mapping_spec(mapping_spec_id=job.mapping_spec_id)
        if not mapping_spec:
            await _fail_job(f"mapping_spec_not_found:{job.mapping_spec_id}")
        if mapping_spec.dataset_id != job.dataset_id:
            await _fail_job("mapping_spec_dataset_mismatch")
        if int(mapping_spec.version) != int(job.mapping_spec_version):
            await _fail_job(
                f"mapping_spec_version_mismatch(job={job.mapping_spec_version} spec={mapping_spec.version})"
            )

        await self.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status="RUNNING",
        )

        options: Dict[str, Any] = dict(mapping_spec.options or {})
        if isinstance(job.options, dict):
            options.update(job.options)

        max_rows = job.max_rows if job.max_rows is not None else options.get("max_rows")
        if max_rows is None:
            max_rows = self.max_rows_default
        try:
            max_rows = int(max_rows)
        except Exception:
            max_rows = self.max_rows_default
        if max_rows <= 0:
            max_rows = None

        batch_size = int(job.batch_size or options.get("batch_size") or self.batch_size_default)
        batch_size = max(1, min(batch_size, 5000))
        row_batch_size = int(options.get("row_batch_size") or options.get("read_batch_size") or self.row_batch_size_default)
        row_batch_size = max(1, min(row_batch_size, 50000))
        allow_partial = bool(job.allow_partial)

        target_field_types = mapping_spec.target_field_types or {}
        if not target_field_types:
            target_field_types = await self._fetch_target_field_types(job)
        else:
            expected_id_key = f"{job.target_class_id.lower()}_id"
            if expected_id_key not in target_field_types:
                fetched_field_types = await self._fetch_target_field_types(job)
                for key, dtype in fetched_field_types.items():
                    target_field_types.setdefault(key, dtype)

        mappings = [
            FieldMapping(source_field=str(m.get("source_field") or ""), target_field=str(m.get("target_field") or ""))
            for m in mapping_spec.mappings
            if isinstance(m, dict)
        ]
        pk_fields = self._normalize_pk_fields(
            options.get("primary_key_fields")
            or options.get("primary_keys")
            or options.get("row_pk_fields")
            or options.get("pk_fields")
            or options.get("primary_key_columns")
            or options.get("row_pk_columns")
        )
        pk_targets = self._normalize_pk_fields(
            options.get("primary_key_targets") or options.get("target_primary_keys")
        )

        ontology_version = await self._fetch_ontology_version(job)
        job_node_id = await self._record_lineage_header(
            job=job,
            mapping_spec=mapping_spec,
            ontology_version=ontology_version,
            input_type=input_type,
            artifact_output_name=resolved_output_name,
        )

        validation_errors: List[Dict[str, Any]] = []
        validation_error_rows: List[int] = []
        validation_stats: Dict[str, Any] = {}
        validated_total_rows = 0

        if not allow_partial:
            (
                validated_total_rows,
                validation_errors,
                validation_error_rows,
                validation_stats,
            ) = await self._validate_batches(
                job=job,
                options=options,
                mappings=mappings,
                target_field_types=target_field_types,
                row_batch_size=row_batch_size,
                max_rows=max_rows,
            )
            if validated_total_rows == 0:
                await _fail_job("no_rows_loaded")
            if validation_errors:
                await _fail_job(
                    "validation_failed",
                    report={
                        "errors": validation_errors[:200],
                        "stats": validation_stats,
                        "error_row_indices": validation_error_rows[:200],
                    },
                )

        command_ids: List[str] = []
        prepared_instances = 0
        instance_ids_sample: List[str] = []
        error_rows: List[int] = []
        errors: List[Dict[str, Any]] = []
        total_rows_seen = 0
        lineage_remaining = self.lineage_max_links

        async for columns, rows, row_offset in self._iter_dataset_batches(
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            if not rows:
                continue
            total_rows_seen += len(rows)
            build = SheetImportService.build_instances(
                columns=columns,
                rows=rows,
                mappings=mappings,
                target_field_types=target_field_types,
            )
            batch_errors = build.get("errors") or []
            if batch_errors:
                for err in batch_errors:
                    if isinstance(err, dict) and "row_index" in err:
                        try:
                            err["row_index"] = int(err["row_index"]) + int(row_offset)
                        except Exception:
                            pass
                if allow_partial:
                    remaining = max(0, 200 - len(errors))
                    if remaining:
                        errors.extend(batch_errors[:remaining])
                else:
                    await _fail_job("validation_failed", report={"errors": batch_errors[:200]})

            instances = build.get("instances") or []
            instance_row_indices = build.get("instance_row_indices") or []
            error_row_indices = set(build.get("error_row_indices") or [])

            if error_row_indices:
                for idx in sorted(error_row_indices):
                    try:
                        error_rows.append(int(row_offset) + int(idx))
                    except Exception:
                        continue
                if allow_partial:
                    filtered_instances = []
                    filtered_indices = []
                    for inst, row_idx in zip(instances, instance_row_indices):
                        if row_idx in error_row_indices:
                            continue
                        filtered_instances.append(inst)
                        filtered_indices.append(row_idx)
                    instances = filtered_instances
                    instance_row_indices = filtered_indices
                else:
                    await _fail_job("validation_failed", report={"errors": batch_errors[:200]})

            if not instances:
                continue

            col_index = SheetImportService.build_column_index(columns)
            row_keys: List[str] = []
            for inst, row_idx in zip(instances, instance_row_indices):
                row = rows[row_idx] if row_idx < len(rows) else None
                row_keys.append(
                    self._derive_row_key(
                        columns=columns,
                        col_index=col_index,
                        row=row,
                        instance=inst,
                        pk_fields=pk_fields,
                        pk_targets=pk_targets,
                    )
                )

            instance_id_field = None
            if pk_targets:
                for target in pk_targets:
                    if target in target_field_types:
                        instance_id_field = target
                        break
            if not instance_id_field:
                for candidate in (f"{job.target_class_id.lower()}_id", "id"):
                    if candidate in target_field_types:
                        instance_id_field = candidate
                        break

            instances, instance_ids = self._ensure_instance_ids(
                instances,
                class_id=job.target_class_id,
                stable_seed=stable_seed or job.job_id,
                mapping_spec_version=job.mapping_spec_version,
                row_keys=row_keys,
                instance_id_field=instance_id_field,
            )

            for idx in range(0, len(instances), batch_size):
                batch = instances[idx : idx + batch_size]
                resp = await self._bulk_create_instances(job, batch, ontology_version=ontology_version)
                command_id = resp.get("command_id") if isinstance(resp, dict) else None
                if command_id:
                    command_ids.append(str(command_id))

            prepared_instances += len(instances)
            if len(instance_ids_sample) < 10:
                instance_ids_sample.extend(instance_ids[: max(0, 10 - len(instance_ids_sample))])

            lineage_remaining = await self._record_instance_lineage(
                job=job,
                job_node_id=job_node_id,
                instance_ids=instance_ids,
                mapping_spec_id=mapping_spec.mapping_spec_id,
                mapping_spec_version=mapping_spec.version,
                ontology_version=ontology_version,
                limit_remaining=lineage_remaining,
                input_type=input_type,
                artifact_output_name=resolved_output_name,
            )

        total_rows = validated_total_rows if not allow_partial else total_rows_seen
        if total_rows == 0:
            await _fail_job("no_rows_loaded")
        if prepared_instances == 0:
            await _fail_job(
                "no_valid_instances",
                report={
                    "errors": (errors or validation_errors)[:200],
                    "error_row_indices": error_rows[:200],
                },
            )

        await self.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status="SUBMITTED",
            command_id=command_ids[0] if command_ids else None,
            report={
                "total_rows": total_rows,
                "prepared_instances": prepared_instances,
                "errors": (errors or validation_errors)[:200],
                "error_row_indices": (error_rows or validation_error_rows)[:200],
                "command_ids": command_ids,
                "instance_ids_sample": instance_ids_sample[:10],
                "ontology_version": ontology_version or {},
            },
            completed_at=datetime.now(timezone.utc),
        )

    async def _bulk_create_instances(
        self,
        job: ObjectifyJob,
        instances: List[Dict[str, Any]],
        *,
        ontology_version: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        if not self.http:
            raise RuntimeError("OMS client not initialized")
        branch = job.ontology_branch or job.dataset_branch or "main"
        metadata = {
            "objectify_job_id": job.job_id,
            "mapping_spec_id": job.mapping_spec_id,
            "mapping_spec_version": job.mapping_spec_version,
            "dataset_id": job.dataset_id,
            "dataset_version_id": job.dataset_version_id,
            "artifact_id": job.artifact_id,
            "artifact_output_name": job.artifact_output_name,
            "options": job.options,
        }
        if ontology_version:
            metadata["ontology"] = ontology_version
        resp = await self.http.post(
            f"/api/v1/instances/{job.db_name}/async/{job.target_class_id}/bulk-create",
            params={"branch": branch},
            json={
                "instances": instances,
                "metadata": metadata,
            },
        )
        resp.raise_for_status()
        if not resp.text:
            return {}
        return resp.json()

    async def _resolve_artifact_output(self, job: ObjectifyJob) -> Tuple[str, str]:
        if not self.pipeline_registry:
            raise RuntimeError("PipelineRegistry not initialized")
        if not job.artifact_id:
            raise ValueError("artifact_id is required")
        artifact = await self.pipeline_registry.get_artifact(artifact_id=job.artifact_id)
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

    async def _fetch_ontology_version(self, job: ObjectifyJob) -> Dict[str, str]:
        if not self.http:
            return {}
        branch = job.ontology_branch or job.dataset_branch or "main"
        resp = await self.http.get(
            f"/api/v1/version/{job.db_name}/head",
            params={"branch": branch},
        )
        if resp.status_code == 404:
            return {"ref": f"branch:{branch}"}
        resp.raise_for_status()
        payload = resp.json() if resp.text else {}
        data = payload.get("data") if isinstance(payload, dict) else {}
        head_commit = None
        if isinstance(data, dict):
            head_commit = data.get("head_commit_id") or data.get("commit")
        if head_commit:
            return {"ref": f"branch:{branch}", "commit": str(head_commit)}
        return {"ref": f"branch:{branch}"}

    @staticmethod
    def _normalize_pk_fields(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return []

    @staticmethod
    def _hash_payload(payload: Any) -> str:
        raw = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _derive_row_key(
        self,
        *,
        columns: List[Any],
        col_index: Dict[str, int],
        row: Optional[List[Any]],
        instance: Dict[str, Any],
        pk_fields: List[str],
        pk_targets: List[str],
    ) -> str:
        if pk_targets:
            if all(field in instance and instance.get(field) is not None for field in pk_targets):
                values = [str(instance.get(field)) for field in pk_targets]
                return f"target:{'|'.join(values)}"
        if pk_fields:
            if all(field in instance and instance.get(field) is not None for field in pk_fields):
                values = [str(instance.get(field)) for field in pk_fields]
                return f"target:{'|'.join(values)}"
            if row is not None and all(field in col_index for field in pk_fields):
                values = []
                for field in pk_fields:
                    idx = col_index.get(field)
                    values.append(str(row[idx]) if idx is not None and idx < len(row) else "")
                return f"source:{'|'.join(values)}"

        if row is not None and columns:
            payload = {str(columns[i]): row[i] if i < len(row) else None for i in range(len(columns))}
        else:
            payload = instance or {}
        return f"hash:{self._hash_payload(payload)}"

    async def _iter_dataset_batches(
        self,
        *,
        job: ObjectifyJob,
        options: Dict[str, Any],
        row_batch_size: int,
        max_rows: Optional[int],
    ):
        if not self.storage:
            raise RuntimeError("Storage service not initialized")
        parsed = parse_s3_uri(job.artifact_key)
        if not parsed:
            raise ValueError(f"Invalid artifact_key: {job.artifact_key}")
        bucket, key = parsed
        has_header = options.get("source_has_header", True)
        delimiter = options.get("delimiter") or options.get("csv_delimiter") or ","

        if key.endswith(".csv") or key.endswith(".tsv") or key.endswith(".txt"):
            async for columns, rows, row_offset in self._iter_csv_batches(
                bucket=bucket,
                key=key,
                delimiter=delimiter,
                has_header=bool(has_header),
                row_batch_size=row_batch_size,
                max_rows=max_rows,
            ):
                yield columns, rows, row_offset
            return

        if key.endswith(".xlsx") or key.endswith(".xlsm") or key.endswith(".xls"):
            raise RuntimeError("Excel artifacts are not supported for objectify; convert via pipeline first.")

        async for columns, rows, row_offset in self._iter_json_part_batches(
            bucket=bucket,
            prefix=key,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            yield columns, rows, row_offset

    async def _iter_csv_batches(
        self,
        *,
        bucket: str,
        key: str,
        delimiter: str,
        has_header: bool,
        row_batch_size: int,
        max_rows: Optional[int],
    ):
        if not self.storage:
            raise RuntimeError("Storage service not initialized")

        q: queue_module.Queue = queue_module.Queue(maxsize=2)
        stop_flag = threading.Event()

        def _safe_put(item: Tuple[str, Any, Any, Any]) -> None:
            while not stop_flag.is_set():
                try:
                    q.put(item, timeout=0.5)
                    return
                except queue_module.Full:
                    continue

        def _reader() -> None:
            body = None
            try:
                resp = self.storage.client.get_object(Bucket=bucket, Key=key)
                body = resp.get("Body")
                text_stream = io.TextIOWrapper(body, encoding="utf-8", errors="replace")
                reader = csv.reader(text_stream, delimiter=delimiter)

                columns: List[str] = []
                rows: List[List[Any]] = []
                row_offset = 0
                seen_rows = 0

                if has_header:
                    header = next(reader, None)
                    if header:
                        columns = [str(c).strip() for c in header]
                else:
                    first = next(reader, None)
                    if first is None:
                        _safe_put(("done", columns, [], row_offset))
                        return
                    columns = [f"col_{i}" for i in range(len(first))]
                    rows.append(first)
                    seen_rows += 1

                for row in reader:
                    if stop_flag.is_set():
                        break
                    if max_rows is not None and seen_rows >= max_rows:
                        break
                    if not columns:
                        columns = [f"col_{i}" for i in range(len(row))]
                    rows.append(row)
                    seen_rows += 1
                    if len(rows) >= row_batch_size:
                        _safe_put(("batch", columns, rows, row_offset))
                        row_offset += len(rows)
                        rows = []

                if rows and not stop_flag.is_set():
                    _safe_put(("batch", columns, rows, row_offset))
                    row_offset += len(rows)
                _safe_put(("done", columns, [], row_offset))
            except Exception as exc:
                _safe_put(("error", exc, None, None))
            finally:
                try:
                    if body:
                        body.close()
                except Exception:
                    pass

        thread = threading.Thread(target=_reader, daemon=True)
        thread.start()

        try:
            while True:
                kind, columns, rows, row_offset = await asyncio.to_thread(q.get)
                if kind == "error":
                    raise columns
                if kind == "done":
                    break
                if kind == "batch":
                    yield columns, rows, row_offset
        finally:
            stop_flag.set()

    async def _iter_json_part_batches(
        self,
        *,
        bucket: str,
        prefix: str,
        row_batch_size: int,
        max_rows: Optional[int],
    ):
        if not self.storage:
            raise RuntimeError("Storage service not initialized")
        rows: List[List[Any]] = []
        columns: List[str] = []
        row_offset = 0
        total_rows = 0

        async for obj in self.storage.iter_objects(bucket=bucket, prefix=prefix, max_keys=self.list_page_size):
            obj_key = obj.get("Key")
            if not obj_key or obj_key.endswith("/"):
                continue
            if not obj_key.startswith(prefix):
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
                    row = [payload.get(col) for col in columns]
                elif isinstance(payload, list):
                    if not columns:
                        columns = [f"col_{i}" for i in range(len(payload))]
                    row = payload
                else:
                    continue

                rows.append(row)
                total_rows += 1
                if max_rows is not None and total_rows >= max_rows:
                    break
                if len(rows) >= row_batch_size:
                    yield columns, rows, row_offset
                    row_offset += len(rows)
                    rows = []

            if max_rows is not None and total_rows >= max_rows:
                break

        if rows:
            yield columns, rows, row_offset

    async def _validate_batches(
        self,
        *,
        job: ObjectifyJob,
        options: Dict[str, Any],
        mappings: List[FieldMapping],
        target_field_types: Dict[str, str],
        row_batch_size: int,
        max_rows: Optional[int],
    ) -> Tuple[int, List[Dict[str, Any]], List[int], Dict[str, Any]]:
        errors: List[Dict[str, Any]] = []
        error_row_indices: List[int] = []
        total_rows = 0
        error_count = 0

        async for columns, rows, row_offset in self._iter_dataset_batches(
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            if not rows:
                continue
            total_rows += len(rows)
            build = SheetImportService.build_instances(
                columns=columns,
                rows=rows,
                mappings=mappings,
                target_field_types=target_field_types,
            )
            batch_errors = build.get("errors") or []
            for err in batch_errors:
                if isinstance(err, dict) and "row_index" in err:
                    try:
                        err["row_index"] = int(err["row_index"]) + int(row_offset)
                    except Exception:
                        pass
                if len(errors) < 200:
                    errors.append(err)
            batch_error_rows = build.get("error_row_indices") or []
            for idx in batch_error_rows:
                try:
                    error_row_indices.append(int(row_offset) + int(idx))
                except Exception:
                    continue
            error_count += len(batch_errors)

        stats = {
            "input_rows": total_rows,
            "error_rows": len(error_row_indices),
            "error_count": error_count,
        }
        return total_rows, errors, error_row_indices, stats

    def _ensure_instance_ids(
        self,
        instances: List[Dict[str, Any]],
        *,
        class_id: str,
        stable_seed: str,
        mapping_spec_version: int,
        row_keys: Optional[List[str]] = None,
        instance_id_field: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        expected_key = instance_id_field or f"{class_id.lower()}_id"
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
                row_key = row_keys[idx] if row_keys and idx < len(row_keys) else str(idx)
                seed = f"{stable_seed}:{mapping_spec_version}:{row_key}"
                suffix = uuid5(NAMESPACE_URL, f"objectify:{seed}").hex[:12]
                candidate = f"{class_id.lower()}_{suffix}"
                inst[expected_key] = candidate
            instance_ids.append(str(candidate))
        return instances, instance_ids

    async def _record_lineage_header(
        self,
        *,
        job: ObjectifyJob,
        mapping_spec: Any,
        ontology_version: Optional[Dict[str, str]],
        input_type: str,
        artifact_output_name: Optional[str] = None,
    ) -> Optional[str]:
        if not self.lineage_store:
            return None
        try:
            job_node = self.lineage_store.node_aggregate("ObjectifyJob", job.job_id)
            if input_type == "artifact":
                source_node = self.lineage_store.node_aggregate("PipelineArtifact", str(job.artifact_id))
                edge_type = "pipeline_artifact_objectify_job"
                edge_metadata = {
                    "db_name": job.db_name,
                    "dataset_id": job.dataset_id,
                    "artifact_id": job.artifact_id,
                    "artifact_output_name": artifact_output_name or job.artifact_output_name,
                    "mapping_spec_id": job.mapping_spec_id,
                    "mapping_spec_version": job.mapping_spec_version,
                    "target_class_id": job.target_class_id,
                }
            else:
                source_node = self.lineage_store.node_aggregate("DatasetVersion", str(job.dataset_version_id))
                edge_type = "dataset_version_objectify_job"
                edge_metadata = {
                    "db_name": job.db_name,
                    "dataset_id": job.dataset_id,
                    "dataset_version_id": job.dataset_version_id,
                    "mapping_spec_id": job.mapping_spec_id,
                    "mapping_spec_version": job.mapping_spec_version,
                    "target_class_id": job.target_class_id,
                }
            await self.lineage_store.record_link(
                from_node_id=source_node,
                to_node_id=job_node,
                edge_type=edge_type,
                occurred_at=datetime.now(timezone.utc),
                db_name=job.db_name,
                edge_metadata=edge_metadata,
            )

            mapping_version_id = f"{mapping_spec.mapping_spec_id}:v{mapping_spec.version}"
            mapping_node = self.lineage_store.node_aggregate("MappingSpecVersion", mapping_version_id)
            await self.lineage_store.record_link(
                from_node_id=job_node,
                to_node_id=mapping_node,
                edge_type="objectify_job_mapping_spec",
                occurred_at=datetime.now(timezone.utc),
                db_name=job.db_name,
                edge_metadata={
                    "db_name": job.db_name,
                    "mapping_spec_id": mapping_spec.mapping_spec_id,
                    "mapping_spec_version": mapping_spec.version,
                    "dataset_id": job.dataset_id,
                },
            )

            if ontology_version:
                branch = job.ontology_branch or job.dataset_branch or "main"
                ont_id = f"{job.db_name}:{branch}:{ontology_version.get('commit') or 'head'}"
                ont_node = self.lineage_store.node_aggregate("OntologyVersion", ont_id)
                await self.lineage_store.record_link(
                    from_node_id=job_node,
                    to_node_id=ont_node,
                    edge_type="objectify_job_ontology_version",
                    occurred_at=datetime.now(timezone.utc),
                    db_name=job.db_name,
                    edge_metadata={
                        "db_name": job.db_name,
                        "branch": branch,
                        "ontology": ontology_version,
                    },
                )
            return job_node
        except Exception as exc:
            logger.warning("Failed to record objectify job lineage header: %s", exc)
            return None

    async def _record_instance_lineage(
        self,
        *,
        job: ObjectifyJob,
        job_node_id: Optional[str],
        instance_ids: List[str],
        mapping_spec_id: str,
        mapping_spec_version: int,
        ontology_version: Optional[Dict[str, str]],
        limit_remaining: int,
        input_type: str,
        artifact_output_name: Optional[str] = None,
    ) -> int:
        if not self.lineage_store:
            return limit_remaining
        if limit_remaining <= 0:
            return limit_remaining
        if input_type == "artifact":
            source_node = self.lineage_store.node_aggregate("PipelineArtifact", str(job.artifact_id))
            edge_type = "pipeline_artifact_objectified"
            edge_metadata = {
                "db_name": job.db_name,
                "dataset_id": job.dataset_id,
                "artifact_id": job.artifact_id,
                "artifact_output_name": artifact_output_name or job.artifact_output_name,
                "mapping_spec_id": mapping_spec_id,
                "mapping_spec_version": mapping_spec_version,
                "target_class_id": job.target_class_id,
                "ontology": ontology_version or {},
            }
        else:
            source_node = self.lineage_store.node_aggregate("DatasetVersion", str(job.dataset_version_id))
            edge_type = "dataset_version_objectified"
            edge_metadata = {
                "db_name": job.db_name,
                "dataset_id": job.dataset_id,
                "dataset_version_id": job.dataset_version_id,
                "mapping_spec_id": mapping_spec_id,
                "mapping_spec_version": mapping_spec_version,
                "target_class_id": job.target_class_id,
                "ontology": ontology_version or {},
            }
        for instance_id in instance_ids:
            if limit_remaining <= 0:
                break
            aggregate_id = f"{job.db_name}:{job.dataset_branch}:{job.target_class_id}:{instance_id}"
            instance_node = self.lineage_store.node_aggregate("Instance", aggregate_id)
            try:
                await self.lineage_store.record_link(
                    from_node_id=source_node,
                    to_node_id=instance_node,
                    edge_type=edge_type,
                    occurred_at=datetime.now(timezone.utc),
                    db_name=job.db_name,
                    edge_metadata=edge_metadata,
                )
                if job_node_id:
                    await self.lineage_store.record_link(
                        from_node_id=job_node_id,
                        to_node_id=instance_node,
                        edge_type="objectify_job_created_instance",
                        occurred_at=datetime.now(timezone.utc),
                        db_name=job.db_name,
                        edge_metadata={
                            "db_name": job.db_name,
                            "dataset_id": job.dataset_id,
                            "dataset_version_id": job.dataset_version_id,
                            "artifact_id": job.artifact_id,
                            "artifact_output_name": artifact_output_name or job.artifact_output_name,
                            "mapping_spec_id": mapping_spec_id,
                            "mapping_spec_version": mapping_spec_version,
                            "target_class_id": job.target_class_id,
                            "ontology": ontology_version or {},
                        },
                    )
                limit_remaining -= 1
            except Exception as exc:
                logger.warning("Failed to record lineage for instance %s: %s", instance_id, exc)
        return limit_remaining

    async def _heartbeat_loop(self, *, handler: str, event_id: str) -> None:
        if not self.processed:
            return
        interval = parse_int_env("PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS", 30, min_value=1, max_value=3600)
        while True:
            try:
                await asyncio.sleep(interval)
                await self.processed.heartbeat(handler=handler, event_id=event_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Objectify heartbeat failed (handler=%s event_id=%s): %s", handler, event_id, exc)

    async def _send_to_dlq(
        self,
        *,
        job: Optional[ObjectifyJob],
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
    ) -> None:
        if not self.dlq_producer:
            logger.error("DLQ producer not configured; dropping objectify DLQ payload: %s", error)
            return
        payload = {
            "kind": "objectify_job_dlq",
            "job": job.model_dump(mode="json") if job else None,
            "error": error,
            "attempt_count": int(attempt_count),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "raw_payload": raw_payload,
        }
        key = (job.job_id if job else "objectify-job").encode("utf-8")
        value = json.dumps(payload, ensure_ascii=True, default=str).encode("utf-8")
        self.dlq_producer.produce(self.dlq_topic, key=key, value=value)
        self.dlq_producer.flush(10)

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        if isinstance(exc, ObjectifyNonRetryableError):
            return False
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            if status == 429 or status >= 500:
                return True
            return False
        if isinstance(exc, httpx.RequestError):
            return True
        msg = str(exc).lower()
        non_retryable_markers = [
            "validation_failed",
            "mapping_spec_not_found",
            "mapping_spec_dataset_mismatch",
            "dataset_version_mismatch",
            "dataset_not_found",
            "db_name_mismatch",
            "artifact_key_mismatch",
            "invalid artifact_key",
            "invalid_artifact_key",
            "artifact_not_found",
            "artifact_not_success",
            "artifact_not_build",
            "artifact_outputs_missing",
            "artifact_output_not_found",
            "artifact_output_ambiguous",
            "artifact_output_name_required",
            "artifact_key_missing",
            "objectify_input_conflict",
            "objectify_input_missing",
            "no_rows_loaded",
        ]
        return not any(marker in msg for marker in non_retryable_markers)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    worker = ObjectifyWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
