"""
Pipeline Worker (Spark/Flink-ready execution runtime).

Consumes Kafka pipeline-jobs and executes dataset transforms using Spark.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.models.pipeline_job import PipelineJob
from shared.services.dataset_registry import DatasetRegistry
from shared.services.lineage_store import LineageStore
from shared.services.pipeline_job_queue import PipelineJobQueue
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.processed_event_registry import ProcessedEventRegistry, ClaimDecision
from shared.services.storage_service import StorageService
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri

logger = logging.getLogger(__name__)


def _get_bff_token() -> Optional[str]:
    for key in ("BFF_ADMIN_TOKEN", "BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PipelineWorker:
    def __init__(self) -> None:
        self.running = False
        self.topic = (os.getenv("PIPELINE_JOBS_TOPIC") or "pipeline-jobs").strip() or "pipeline-jobs"
        self.group_id = (os.getenv("PIPELINE_JOBS_GROUP") or "pipeline-worker-group").strip()
        self.handler = (os.getenv("PIPELINE_WORKER_HANDLER") or "pipeline_worker").strip()
        self.pipeline_label = (os.getenv("PIPELINE_WORKER_NAME") or "pipeline_worker").strip()

        self.consumer: Optional[Consumer] = None
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.pipeline_registry: Optional[PipelineRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.lineage: Optional[LineageStore] = None
        self.storage: Optional[StorageService] = None
        self.sheets: Optional[GoogleSheetsService] = None
        self.http: Optional[httpx.AsyncClient] = None
        self.spark: Optional[SparkSession] = None
        self.job_queue: Optional[PipelineJobQueue] = None

    async def initialize(self) -> None:
        self.dataset_registry = DatasetRegistry()
        await self.dataset_registry.initialize()

        self.pipeline_registry = PipelineRegistry()
        await self.pipeline_registry.initialize()

        self.processed = ProcessedEventRegistry()
        await self.processed.initialize()

        self.lineage = LineageStore()
        try:
            await self.lineage.initialize()
        except Exception as exc:
            logger.warning("LineageStore unavailable: %s", exc)
            self.lineage = None

        from shared.services.storage_service import create_storage_service
        from shared.config.settings import ApplicationSettings

        settings = ApplicationSettings()
        self.storage = create_storage_service(settings)
        if not self.storage:
            raise RuntimeError("Storage service is not configured")

        api_key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_SHEETS_API_KEY") or "").strip() or None
        self.sheets = GoogleSheetsService(api_key=api_key)

        token = _get_bff_token()
        headers: Dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
            headers["X-Admin-Token"] = token
        self.http = httpx.AsyncClient(timeout=120.0, headers=headers)

        self.spark = (
            SparkSession.builder.appName("spice-pipeline-worker")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        self.job_queue = PipelineJobQueue()

        self.consumer = Consumer(
            {
                "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 45000,
                "max.poll.interval.ms": 300000,
            }
        )
        self.consumer.subscribe([self.topic])
        logger.info("PipelineWorker initialized (topic=%s)", self.topic)

    async def close(self) -> None:
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.http:
            await self.http.aclose()
            self.http = None
        if self.sheets:
            await self.sheets.close()
            self.sheets = None
        if self.lineage:
            await self.lineage.close()
            self.lineage = None
        if self.processed:
            await self.processed.close()
            self.processed = None
        if self.pipeline_registry:
            await self.pipeline_registry.close()
            self.pipeline_registry = None
        if self.dataset_registry:
            await self.dataset_registry.close()
            self.dataset_registry = None
        if self.spark:
            self.spark.stop()
            self.spark = None

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        try:
            while self.running:
                msg = self.consumer.poll(1.0) if self.consumer else None
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    job = PipelineJob.model_validate(payload)
                    await self._handle_job(job)
                    if self.consumer:
                        self.consumer.commit(msg)
                except Exception as exc:
                    logger.exception("Failed to process pipeline job: %s", exc)
        finally:
            await self.close()

    async def _handle_job(self, job: PipelineJob) -> None:
        if not self.processed:
            raise RuntimeError("ProcessedEventRegistry not available")

        claim = await self.processed.claim(
            handler=self.handler,
            event_id=job.job_id,
            aggregate_id=job.pipeline_id,
        )
        if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE, ClaimDecision.IN_PROGRESS}:
            return

        try:
            await self._execute_job(job)
            await self.processed.mark_done(handler=self.handler, event_id=job.job_id)
        except Exception as exc:
            await self.processed.mark_failed(handler=self.handler, event_id=job.job_id, error=str(exc))
            raise

    async def _execute_job(self, job: PipelineJob) -> None:
        if not self.pipeline_registry or not self.dataset_registry:
            raise RuntimeError("Registry services not available")
        if not self.spark:
            raise RuntimeError("Spark session not initialized")
        if not self.storage:
            raise RuntimeError("Storage service not available")

        definition = job.definition_json or {}
        tables: Dict[str, DataFrame] = {}
        nodes = _normalize_nodes(definition.get("nodes"))
        edges = _normalize_edges(definition.get("edges"))
        order = _topological_sort(nodes, edges)
        incoming = _build_incoming(edges)
        parameters = _normalize_parameters(definition.get("parameters"))
        temp_dirs: list[str] = []

        await self.pipeline_registry.record_build(
            pipeline_id=job.pipeline_id,
            status="RUNNING",
            output_json={"job_id": job.job_id},
        )

        try:
            for node_id in order:
                node = nodes[node_id]
                metadata = node.get("metadata") or {}
                node_type = str(node.get("type") or "transform")
                inputs = [tables[src] for src in incoming.get(node_id, []) if src in tables]

                if node_type == "input":
                    df = await self._load_input_dataframe(job.db_name, metadata, temp_dirs)
                elif node_type == "output":
                    df = inputs[0] if inputs else self._empty_dataframe()
                else:
                    df = self._apply_transform(metadata, inputs, parameters)

                tables[node_id] = df

            output_node_id = job.node_id or next(
                (nid for nid, node in nodes.items() if node.get("type") == "output"),
                None,
            )
            if output_node_id and output_node_id in tables:
                output_df = tables[output_node_id]
            elif tables:
                output_df = list(tables.values())[-1]
            else:
                output_df = self._empty_dataframe()

            schema_columns = _schema_from_dataframe(output_df)
            row_count = int(output_df.count())
            sample_rows = output_df.limit(200).collect()
            output_sample = [row.asDict(recursive=True) for row in sample_rows]

            artifact_bucket = os.getenv("PIPELINE_ARTIFACT_BUCKET", "pipeline-artifacts")
            timestamp = _utcnow().strftime("%Y%m%dT%H%M%SZ")
            safe_name = job.output_dataset_name.replace(" ", "_")
            prefix = f"pipelines/{job.db_name}/{job.pipeline_id}/{safe_name}/{timestamp}"
            await self.storage.create_bucket(artifact_bucket)
            temp_dir = tempfile.mkdtemp(prefix="pipeline-output-")
            try:
                output_path = os.path.join(temp_dir, "data")
                output_df.write.mode("overwrite").json(output_path)
                part_files = _list_part_files(output_path)
                if not part_files:
                    raise FileNotFoundError("Spark output part files not found")
                for part_file in part_files:
                    target_key = f"{prefix}/{os.path.basename(part_file)}"
                    with open(part_file, "rb") as handle:
                        await self.storage.save_bytes(
                            artifact_bucket,
                            target_key,
                            handle.read(),
                            content_type="application/json",
                        )
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
            artifact_key = build_s3_uri(artifact_bucket, prefix)

            dataset = await self.dataset_registry.get_dataset_by_name(db_name=job.db_name, name=job.output_dataset_name)
            if not dataset:
                dataset = await self.dataset_registry.create_dataset(
                    db_name=job.db_name,
                    name=job.output_dataset_name,
                    description=None,
                    source_type="pipeline",
                    source_ref=job.pipeline_id,
                    schema_json={"columns": schema_columns},
                )
            await self.dataset_registry.add_version(
                dataset_id=dataset.dataset_id,
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json={"columns": schema_columns, "rows": output_sample},
                schema_json={"columns": schema_columns},
            )

            await self.pipeline_registry.record_build(
                pipeline_id=job.pipeline_id,
                status="DEPLOYED",
                output_json={"artifact_key": artifact_key, "row_count": row_count},
                deployed_version=None,
            )

            if self.lineage:
                parsed = parse_s3_uri(artifact_key)
                if parsed:
                    bucket, key = parsed
                    await self.lineage.record_link(
                        from_node_id=self.lineage.node_aggregate("Pipeline", job.pipeline_id),
                        to_node_id=self.lineage.node_artifact("s3", bucket, key),
                        edge_type="pipeline_output_stored",
                        occurred_at=_utcnow(),
                        db_name=job.db_name,
                        edge_metadata={
                            "db_name": job.db_name,
                            "pipeline_id": job.pipeline_id,
                            "artifact_key": artifact_key,
                        },
                    )
        finally:
            for path in temp_dirs:
                shutil.rmtree(path, ignore_errors=True)

        if job.schedule_interval_seconds and self.job_queue:
            asyncio.create_task(self._schedule_followup(job))

    async def _load_input_dataframe(
        self,
        db_name: str,
        metadata: Dict[str, Any],
        temp_dirs: list[str],
    ) -> DataFrame:
        if not self.dataset_registry:
            raise RuntimeError("Dataset registry not initialized")
        dataset_id = metadata.get("datasetId")
        dataset_id = str(dataset_id) if dataset_id else None
        dataset = None
        if dataset_id:
            dataset = await self.dataset_registry.get_dataset(dataset_id=str(dataset_id))
        if not dataset and metadata.get("datasetName"):
            dataset = await self.dataset_registry.get_dataset_by_name(db_name=db_name, name=str(metadata.get("datasetName")))
        if not dataset:
            return self._empty_dataframe()

        version = await self.dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
        if not version or not version.artifact_key:
            return self._empty_dataframe()

        parsed = parse_s3_uri(version.artifact_key)
        if not parsed:
            return self._empty_dataframe()
        bucket, key = parsed

        return await self._load_artifact_dataframe(bucket, key, temp_dirs)

    async def _load_artifact_dataframe(self, bucket: str, key: str, temp_dirs: list[str]) -> DataFrame:
        prefix = key.rstrip("/")
        has_extension = os.path.splitext(prefix)[1] != ""
        if not has_extension:
            return await self._load_prefix_dataframe(bucket, f"{prefix}/", temp_dirs)
        if key.endswith("/"):
            return await self._load_prefix_dataframe(bucket, key, temp_dirs)

        file_path = await self._download_object(bucket, key, temp_dirs)
        return self._read_local_file(file_path)

    async def _load_prefix_dataframe(self, bucket: str, prefix: str, temp_dirs: list[str]) -> DataFrame:
        objects = await self.storage.list_objects(bucket, prefix=prefix)
        keys = [obj.get("Key") for obj in objects or [] if obj.get("Key")]
        data_keys = [key for key in keys if _is_data_object(key)]
        if not data_keys:
            return self._empty_dataframe()

        temp_dir = tempfile.mkdtemp(prefix="pipeline-input-")
        temp_dirs.append(temp_dir)
        local_paths: list[str] = []
        for object_key in data_keys:
            local_paths.append(await self._download_object(bucket, object_key, temp_dirs, temp_dir=temp_dir))

        if any(path.endswith(".json") for path in local_paths):
            return self.spark.read.json(temp_dir)
        if any(path.endswith(".csv") for path in local_paths):
            return self.spark.read.option("header", "true").csv(temp_dir)
        if any(path.endswith((".xlsx", ".xlsm")) for path in local_paths):
            return self._load_excel_path(next(path for path in local_paths if path.endswith((".xlsx", ".xlsm"))))

        return self._empty_dataframe()

    async def _download_object(self, bucket: str, key: str, temp_dirs: list[str], *, temp_dir: Optional[str] = None) -> str:
        if temp_dir is None:
            temp_dir = tempfile.mkdtemp(prefix="pipeline-input-")
            temp_dirs.append(temp_dir)
        filename = os.path.basename(key) or f"artifact-{uuid4().hex}"
        local_path = os.path.join(temp_dir, filename)
        with open(local_path, "wb") as handle:
            self.storage.client.download_fileobj(bucket, key, handle)
        return local_path

    def _read_local_file(self, path: str) -> DataFrame:
        if path.endswith(".csv"):
            return self.spark.read.option("header", "true").csv(path)
        if path.endswith((".xlsx", ".xlsm")):
            return self._load_excel_path(path)
        if path.endswith(".json"):
            return self._load_json_path(path)
        return self._empty_dataframe()

    def _load_excel_path(self, path: str) -> DataFrame:
        import pandas as pd

        frame = pd.read_excel(path)
        return self.spark.createDataFrame(frame)

    def _load_json_path(self, path: str) -> DataFrame:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                head = handle.read(2048)
                handle.seek(0)
                if head.lstrip().startswith("{") and "\"rows\"" in head:
                    payload = json.load(handle)
                    rows = payload.get("rows") or [] if isinstance(payload, dict) else payload
                    if rows:
                        return self.spark.createDataFrame(rows)
                    return self._empty_dataframe()
        except Exception:
            pass
        return self.spark.read.json(path)

    def _empty_dataframe(self) -> DataFrame:
        return self.spark.createDataFrame([], schema=StructType([]))

    def _apply_transform(
        self,
        metadata: Dict[str, Any],
        inputs: List[DataFrame],
        parameters: Dict[str, Any],
    ) -> DataFrame:
        if not inputs:
            return self._empty_dataframe()
        operation = str(metadata.get("operation") or "")
        if operation == "join" and len(inputs) >= 2:
            join_type = str(metadata.get("joinType") or "inner").lower()
            left_key = metadata.get("leftKey") or metadata.get("joinKey")
            right_key = metadata.get("rightKey") or metadata.get("joinKey")
            left = inputs[0]
            right = inputs[1]
            if left_key and right_key:
                if left_key == right_key:
                    return left.join(right, on=[left_key], how=join_type)
                return left.join(right, left[left_key] == right[right_key], how=join_type)
            common = [col for col in left.columns if col in right.columns]
            if common:
                return left.join(right, on=common, how=join_type)
            logger.warning("Join without keys; falling back to cross join")
            return left.join(right, how=join_type)
        if operation == "filter":
            expr = _apply_parameters(str(metadata.get("expression") or ""), parameters)
            if expr:
                return inputs[0].filter(expr)
        if operation == "compute":
            expr = _apply_parameters(str(metadata.get("expression") or ""), parameters)
            if expr and "=" in expr:
                target, formula = [part.strip() for part in expr.split("=", 1)]
                return inputs[0].withColumn(target, F.expr(formula))
        return inputs[0]

    async def _schedule_followup(self, job: PipelineJob) -> None:
        if not self.job_queue or not job.schedule_interval_seconds:
            return
        await asyncio.sleep(job.schedule_interval_seconds)
        next_job = job.model_copy(
            update={
                "job_id": uuid4().hex,
                "requested_at": _utcnow(),
            }
        )
        await self.job_queue.publish(next_job)


def _is_data_object(key: str) -> bool:
    if not key:
        return False
    base = os.path.basename(key)
    if base.startswith("_"):
        return False
    if base.startswith("."):
        return False
    return bool(os.path.splitext(base)[1])


def _schema_from_dataframe(frame: DataFrame) -> List[Dict[str, str]]:
    columns: List[Dict[str, str]] = []
    schema = frame.schema
    for field in schema.fields:
        columns.append({"name": field.name, "type": _spark_type_to_xsd(field.dataType)})
    return columns


def _spark_type_to_xsd(data_type: Any) -> str:
    if isinstance(data_type, BooleanType):
        return "xsd:boolean"
    if isinstance(data_type, (IntegerType, LongType)):
        return "xsd:integer"
    if isinstance(data_type, (FloatType, DoubleType, DecimalType)):
        return "xsd:decimal"
    if isinstance(data_type, (DateType, TimestampType)):
        return "xsd:dateTime"
    return "xsd:string"


def _normalize_nodes(nodes_raw: Any) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    if not isinstance(nodes_raw, list):
        return output
    for node in nodes_raw:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "")
        if not node_id:
            continue
        output[node_id] = node
    return output


def _normalize_edges(edges_raw: Any) -> List[Dict[str, str]]:
    edges: List[Dict[str, str]] = []
    if not isinstance(edges_raw, list):
        return edges
    for edge in edges_raw:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("from") or "")
        dst = str(edge.get("to") or "")
        if not src or not dst:
            continue
        edges.append({"from": src, "to": dst})
    return edges


def _build_incoming(edges: List[Dict[str, str]]) -> Dict[str, List[str]]:
    incoming: Dict[str, List[str]] = {}
    for edge in edges:
        incoming.setdefault(edge["to"], []).append(edge["from"])
    return incoming


def _topological_sort(nodes: Dict[str, Dict[str, Any]], edges: List[Dict[str, str]]) -> List[str]:
    in_degree = {node_id: 0 for node_id in nodes.keys()}
    outgoing: Dict[str, List[str]] = {node_id: [] for node_id in nodes.keys()}
    for edge in edges:
        src = edge["from"]
        dst = edge["to"]
        if src not in nodes or dst not in nodes:
            continue
        outgoing[src].append(dst)
        in_degree[dst] = in_degree.get(dst, 0) + 1

    queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
    order: List[str] = []
    while queue:
        node_id = queue.pop(0)
        order.append(node_id)
        for neighbor in outgoing.get(node_id, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    return order


def _list_part_files(path: str) -> List[str]:
    part_files: List[str] = []
    for root, _, files in os.walk(path):
        for name in files:
            if name.startswith("part-") and name.endswith(".json"):
                part_files.append(os.path.join(root, name))
    return part_files


def _normalize_parameters(parameters_raw: Any) -> Dict[str, Any]:
    if not isinstance(parameters_raw, list):
        return {}
    output: Dict[str, Any] = {}
    for param in parameters_raw:
        if not isinstance(param, dict):
            continue
        name = str(param.get("name") or "").strip()
        if not name:
            continue
        output[name] = param.get("value")
    return output


def _apply_parameters(expression: str, parameters: Dict[str, Any]) -> str:
    if not parameters:
        return expression
    output = expression
    for name, value in parameters.items():
        token = f"${name}"
        if token in output:
            replacement = value
            if isinstance(value, str) and not value.isnumeric() and not value.startswith("'"):
                replacement = f"'{value}'"
            output = output.replace(token, str(replacement))
        brace_token = f"{{{{{name}}}}}"
        if brace_token in output:
            output = output.replace(brace_token, str(value))
    return output


async def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    worker = PipelineWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
