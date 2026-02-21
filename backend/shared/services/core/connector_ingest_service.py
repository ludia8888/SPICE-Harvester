from __future__ import annotations

import csv
import hashlib
import io
import logging
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from data_connector.adapters.import_config_validators import APPEND_MERGE_IMPORT_MODES, TABLE_IMPORT_MODES
from shared.config.settings import get_settings
from shared.models.objectify_job import ObjectifyJob
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.path_utils import safe_lakefs_ref, safe_path_segment
from shared.utils.s3_uri import build_s3_uri
from shared.utils.schema_hash import compute_schema_hash

logger = logging.getLogger(__name__)

_SUPPORTED_IMPORT_MODES = TABLE_IMPORT_MODES


def _row_hash(row: List[Any]) -> str:
    return hashlib.sha256("|".join(str(v) for v in row).encode("utf-8")).hexdigest()


def _dataset_artifact_prefix(*, db_name: str, dataset_id: str, dataset_name: str) -> str:
    safe_name = safe_path_segment(dataset_name)
    return f"datasets/{db_name}/{dataset_id}/{safe_name}"


def _rows_to_csv_bytes(*, columns: List[str], rows: List[List[Any]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    if columns:
        writer.writerow(columns)
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def _parse_csv_bytes(data: bytes) -> Tuple[List[str], List[List[str]]]:
    text = data.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    columns: List[str] = []
    rows: List[List[str]] = []
    for i, row in enumerate(reader):
        if i == 0:
            columns = [str(col) for col in row]
        else:
            rows.append([str(col) for col in row])
    return columns, rows


def _align_row_to_columns(
    row: List[Any],
    *,
    source_columns: List[str],
    target_columns: List[str],
) -> List[Any]:
    if not target_columns:
        return list(row)
    idx_map = {name: idx for idx, name in enumerate(source_columns)}
    aligned: List[Any] = []
    for target in target_columns:
        idx = idx_map.get(target)
        aligned.append(row[idx] if idx is not None and idx < len(row) else None)
    return aligned


def _align_rows_to_columns(
    rows: List[List[Any]],
    *,
    source_columns: List[str],
    target_columns: List[str],
) -> List[List[Any]]:
    return [
        _align_row_to_columns(row, source_columns=source_columns, target_columns=target_columns)
        for row in rows
    ]


def _apply_append_mode(
    existing_columns: List[str],
    existing_rows: List[List[Any]],
    new_columns: List[str],
    new_rows: List[List[Any]],
) -> Tuple[List[str], List[List[Any]]]:
    merged_columns = existing_columns if existing_columns else new_columns
    normalized_new_rows = (
        _align_rows_to_columns(new_rows, source_columns=new_columns, target_columns=merged_columns)
        if merged_columns and new_columns and merged_columns != new_columns
        else new_rows
    )

    existing_hashes = {_row_hash(row) for row in existing_rows}
    merged_rows = list(existing_rows)
    for row in normalized_new_rows:
        row_hash = _row_hash(row)
        if row_hash in existing_hashes:
            continue
        merged_rows.append(row)
        existing_hashes.add(row_hash)
    return merged_columns, merged_rows


def _apply_update_mode(
    existing_columns: List[str],
    existing_rows: List[List[Any]],
    new_columns: List[str],
    new_rows: List[List[Any]],
    *,
    primary_key_column: Optional[str],
) -> Tuple[List[str], List[List[Any]]]:
    merged_columns = existing_columns if existing_columns else new_columns
    normalized_new_rows = (
        _align_rows_to_columns(new_rows, source_columns=new_columns, target_columns=merged_columns)
        if merged_columns and new_columns and merged_columns != new_columns
        else new_rows
    )

    pk_name = str(primary_key_column or "").strip() or (merged_columns[0] if merged_columns else None)
    if pk_name is None:
        return merged_columns, normalized_new_rows
    pk_index = merged_columns.index(pk_name) if pk_name in merged_columns else 0

    merged_rows = list(existing_rows)
    index_by_pk: Dict[str, int] = {}
    for idx, row in enumerate(merged_rows):
        pk = str(row[pk_index]) if pk_index < len(row) else ""
        index_by_pk[pk] = idx

    for row in normalized_new_rows:
        pk = str(row[pk_index]) if pk_index < len(row) else ""
        existing_idx = index_by_pk.get(pk)
        if existing_idx is None:
            index_by_pk[pk] = len(merged_rows)
            merged_rows.append(row)
        else:
            merged_rows[existing_idx] = row

    return merged_columns, merged_rows


def _resolve_raw_repo() -> str:
    repo = str(get_settings().storage.lakefs_raw_repository or "").strip()
    return repo or "raw-datasets"


class ConnectorIngestService:
    def __init__(
        self,
        *,
        dataset_registry: DatasetRegistry,
        pipeline_registry: PipelineRegistry,
        objectify_registry: Optional[ObjectifyRegistry] = None,
        objectify_job_queue: Optional[ObjectifyJobQueue] = None,
    ) -> None:
        self._dataset_registry = dataset_registry
        self._pipeline_registry = pipeline_registry
        self._objectify_registry = objectify_registry
        self._objectify_job_queue = objectify_job_queue

    async def _ensure_branch_exists(self, *, repository: str, branch: str, source_branch: str = "main") -> None:
        if safe_lakefs_ref(branch) == safe_lakefs_ref(source_branch):
            return
        client = await self._pipeline_registry.get_lakefs_client()
        try:
            await client.create_branch(repository=repository, name=safe_lakefs_ref(branch), source=safe_lakefs_ref(source_branch))
        except Exception:
            # Branch may already exist.
            return

    async def _commit(
        self,
        *,
        repository: str,
        branch: str,
        message: str,
        metadata: Dict[str, Any],
        object_key: str,
    ) -> str:
        client = await self._pipeline_registry.get_lakefs_client()
        return await client.commit(
            repository=repository,
            branch=branch,
            message=message,
            metadata=metadata,
        )

    async def _load_existing_csv(
        self,
        *,
        dataset: Any,
        repository: str,
    ) -> Tuple[List[str], List[List[str]]]:
        latest = await self._dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
        if latest is None or not latest.artifact_key:
            return [], []
        branch_name = str(dataset.branch or "main").strip() or "main"
        object_prefix = _dataset_artifact_prefix(
            db_name=dataset.db_name,
            dataset_id=dataset.dataset_id,
            dataset_name=dataset.name,
        )
        object_key = f"{object_prefix}/source.csv"
        storage = await self._pipeline_registry.get_lakefs_storage()
        try:
            payload = await storage.load_bytes(repository, f"{branch_name}/{object_key}")
            return _parse_csv_bytes(payload)
        except Exception:
            return [], []

    async def _maybe_enqueue_objectify_job(
        self,
        *,
        dataset: Any,
        version: Any,
        actor_user_id: Optional[str],
    ) -> Optional[str]:
        if not self._objectify_registry or not self._objectify_job_queue:
            return None
        if not getattr(version, "artifact_key", None):
            return None

        output_name = getattr(dataset, "name", None)
        schema_hash = (
            compute_schema_hash(version.sample_json.get("columns"))
            if isinstance(getattr(version, "sample_json", None), dict)
            else None
        )
        if not schema_hash and isinstance(getattr(dataset, "schema_json", None), dict):
            schema_hash = compute_schema_hash(dataset.schema_json.get("columns") or [])

        mapping_spec = await self._objectify_registry.get_active_mapping_spec(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            artifact_output_name=output_name,
            schema_hash=schema_hash,
        )
        if not mapping_spec or not mapping_spec.auto_sync:
            return None

        dedupe_key = self._objectify_registry.build_dedupe_key(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            dataset_version_id=version.version_id,
            artifact_id=None,
            artifact_output_name=output_name,
        )
        existing = await self._objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
        if existing:
            return existing.job_id

        options = dict(mapping_spec.options or {})
        job = ObjectifyJob(
            job_id=str(uuid4()),
            db_name=dataset.db_name,
            dataset_id=dataset.dataset_id,
            dataset_version_id=version.version_id,
            artifact_output_name=output_name,
            dedupe_key=dedupe_key,
            dataset_branch=dataset.branch,
            artifact_key=version.artifact_key or "",
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            target_class_id=mapping_spec.target_class_id,
            ontology_branch=options.get("ontology_branch"),
            execution_mode="full",
            max_rows=options.get("max_rows"),
            batch_size=options.get("batch_size"),
            allow_partial=bool(options.get("allow_partial")),
            options={**options, "actor_user_id": actor_user_id},
        )
        await self._objectify_job_queue.publish(job, require_delivery=False)
        return job.job_id

    async def ingest_rows(
        self,
        *,
        db_name: str,
        source_type: str,
        source_id: str,
        columns: List[str],
        rows: List[Dict[str, Any]],
        branch: str = "main",
        dataset_name: Optional[str] = None,
        import_mode: str = "SNAPSHOT",
        primary_key_column: Optional[str] = None,
        actor_user_id: Optional[str] = None,
        source_ref: Optional[str] = None,
    ) -> Dict[str, Any]:
        db = str(db_name or "").strip()
        if not db:
            raise ValueError("db_name is required")

        mode = str(import_mode or "SNAPSHOT").strip().upper()
        if mode not in _SUPPORTED_IMPORT_MODES:
            raise ValueError("import_mode must be one of SNAPSHOT|APPEND|UPDATE|INCREMENTAL|CDC|STREAMING")

        branch_name = str(branch or "main").strip() or "main"
        source_ref_value = str(source_ref or f"{source_type}:{source_id}").strip()
        dataset_label = str(dataset_name or f"{source_type}_{source_id}").strip() or f"{source_type}_{source_id}"

        normalized_columns = [str(col).strip() for col in columns if str(col).strip()]
        if not normalized_columns and rows:
            first = rows[0] if isinstance(rows[0], dict) else {}
            normalized_columns = [str(col).strip() for col in first.keys() if str(col).strip()]
        if not normalized_columns:
            raise ValueError("columns are required")

        incoming_rows: List[List[Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            incoming_rows.append([row.get(col) for col in normalized_columns])

        dataset = await self._dataset_registry.get_dataset_by_source_ref(
            db_name=db,
            source_type="connector",
            source_ref=source_ref_value,
            branch=branch_name,
        )
        if dataset is None:
            dataset = await self._dataset_registry.create_dataset(
                db_name=db,
                name=dataset_label,
                description=f"Connector sync: {source_ref_value}",
                source_type="connector",
                source_ref=source_ref_value,
                schema_json={"columns": [{"name": col, "type": "String"} for col in normalized_columns]},
                branch=branch_name,
            )

        final_columns = list(normalized_columns)
        final_rows = list(incoming_rows)

        repository = _resolve_raw_repo()
        if mode in APPEND_MERGE_IMPORT_MODES or mode == "UPDATE":
            existing_columns, existing_rows = await self._load_existing_csv(dataset=dataset, repository=repository)
            if mode == "UPDATE":
                final_columns, final_rows = _apply_update_mode(
                    existing_columns,
                    existing_rows,
                    normalized_columns,
                    incoming_rows,
                    primary_key_column=primary_key_column,
                )
            else:
                final_columns, final_rows = _apply_append_mode(
                    existing_columns,
                    existing_rows,
                    normalized_columns,
                    incoming_rows,
                )

        object_prefix = _dataset_artifact_prefix(
            db_name=dataset.db_name,
            dataset_id=dataset.dataset_id,
            dataset_name=dataset.name,
        )
        object_key = f"{object_prefix}/source.csv"

        await self._ensure_branch_exists(repository=repository, branch=branch_name, source_branch="main")
        storage = await self._pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        payload = _rows_to_csv_bytes(columns=final_columns, rows=final_rows)
        await storage.save_bytes(
            repository,
            f"{branch_name}/{object_key}",
            payload,
            content_type="text/csv",
        )

        commit_id = await self._commit(
            repository=repository,
            branch=branch_name,
            message=f"Connector {mode.lower()} {db}/{dataset.name}",
            metadata={
                "dataset_id": dataset.dataset_id,
                "db_name": db,
                "dataset_name": dataset.name,
                "source_type": source_type,
                "source_ref": source_ref_value,
            },
            object_key=object_key,
        )
        artifact_key = build_s3_uri(repository, f"{commit_id}/{object_key}")

        sample_rows: List[List[Any]] = final_rows[:25]
        version = await self._dataset_registry.add_version(
            dataset_id=dataset.dataset_id,
            lakefs_commit_id=commit_id,
            artifact_key=artifact_key,
            row_count=len(final_rows),
            sample_json={
                "columns": [{"name": col, "type": "String"} for col in final_columns],
                "rows": sample_rows,
            },
            schema_json={"columns": [{"name": col, "type": "String"} for col in final_columns]},
        )

        objectify_job_id = await self._maybe_enqueue_objectify_job(
            dataset=dataset,
            version=version,
            actor_user_id=actor_user_id,
        )

        return {
            "dataset": {
                "dataset_id": dataset.dataset_id,
                "db_name": dataset.db_name,
                "name": dataset.name,
                "branch": dataset.branch,
            },
            "version": {
                "version_id": version.version_id,
                "lakefs_commit_id": version.lakefs_commit_id,
                "artifact_key": version.artifact_key,
                "row_count": version.row_count,
            },
            "objectify_job_id": objectify_job_id,
            "import_mode": mode,
        }
