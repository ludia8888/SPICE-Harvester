"""Google Sheets -> Pipeline Builder service (BFF).

Extracted from `bff.routers.data_connector_pipelining` to keep routers thin and
to centralize connector-to-dataset materialization (Facade pattern).
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, Request
from shared.errors.error_types import ErrorCode, classified_http_exception

import bff.routers.pipeline_datasets_ops as dataset_ops
from bff.routers.data_connector_ops import _build_google_oauth_client, _resolve_google_connection
from data_connector.adapters.factory import (
    ConnectorAdapterFactory,
    connector_kind_from_source_type,
)
from data_connector.adapters.import_config_validators import (
    CDC_COMPAT_IMPORT_MODES,
    normalize_import_mode,
    validate_resource_import_config,
)
from data_connector.adapters.runtime_credentials import resolve_source_runtime_credentials
from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.app_config import AppConfig
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.core.connector_ingest_service import ConnectorIngestService
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.connector_registry import ConnectorMapping, ConnectorRegistry, ConnectorSource
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.dataset_registry_get_or_create import get_or_create_dataset_record
from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.event_store import event_store
from shared.utils.event_utils import build_command_event
from shared.utils.s3_uri import build_s3_uri
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sync mode helpers — APPEND / UPDATE
# ---------------------------------------------------------------------------


def _row_hash(row: List[Any]) -> str:
    """Deterministic hash of a row for deduplication."""
    return hashlib.sha256("|".join(str(v) for v in row).encode("utf-8")).hexdigest()


def _parse_csv_bytes(data: bytes) -> Tuple[List[str], List[List[str]]]:
    """Parse CSV bytes into (columns, rows)."""
    text = data.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    columns: List[str] = []
    rows: List[List[str]] = []
    for i, row in enumerate(reader):
        if i == 0:
            columns = row
        else:
            rows.append(row)
    return columns, rows


def _align_row_to_columns(
    row: List[Any],
    *,
    source_columns: List[str],
    target_columns: List[str],
) -> List[Any]:
    """Project a row from source column order into target column order."""
    if not target_columns:
        return list(row)
    index_by_name = {name: idx for idx, name in enumerate(source_columns)}
    aligned: List[Any] = []
    for name in target_columns:
        idx = index_by_name.get(name)
        if idx is None or idx >= len(row):
            aligned.append(None)
        else:
            aligned.append(row[idx])
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
    """APPEND mode: add only new rows (by hash) that don't exist in the current dataset."""
    # Use new_columns as canonical if existing is empty
    merged_columns = existing_columns if existing_columns else new_columns

    normalized_new_rows = (
        _align_rows_to_columns(new_rows, source_columns=new_columns, target_columns=merged_columns)
        if merged_columns and new_columns and merged_columns != new_columns
        else new_rows
    )

    # Build hash set of existing rows for O(1) lookup
    existing_hashes = {_row_hash(row) for row in existing_rows}

    appended = list(existing_rows)
    for row in normalized_new_rows:
        h = _row_hash(row)
        if h not in existing_hashes:
            appended.append(row)
            existing_hashes.add(h)

    return merged_columns, appended


def _apply_update_mode(
    existing_columns: List[str],
    existing_rows: List[List[Any]],
    new_columns: List[str],
    new_rows: List[List[Any]],
    primary_key_column: Optional[str] = None,
) -> Tuple[List[str], List[List[Any]]]:
    """UPDATE mode: merge/upsert rows by primary key column.

    If primary_key_column is not specified, the first column is used as the PK.
    Existing rows are updated if the PK matches; new rows are appended.
    """
    merged_columns = existing_columns if existing_columns else new_columns

    normalized_new_rows = (
        _align_rows_to_columns(new_rows, source_columns=new_columns, target_columns=merged_columns)
        if merged_columns and new_columns and merged_columns != new_columns
        else new_rows
    )

    # Resolve PK column index
    pk_col = primary_key_column or (merged_columns[0] if merged_columns else None)
    pk_index = 0
    if pk_col and pk_col in merged_columns:
        pk_index = merged_columns.index(pk_col)

    # Build index of existing rows by PK value
    pk_map: Dict[str, int] = {}
    merged_rows = list(existing_rows)
    for idx, row in enumerate(merged_rows):
        pk_value = str(row[pk_index]) if pk_index < len(row) else ""
        pk_map[pk_value] = idx

    for row in normalized_new_rows:
        pk_value = str(row[pk_index]) if pk_index < len(row) else ""
        if pk_value in pk_map:
            # Update existing row
            merged_rows[pk_map[pk_value]] = row
        else:
            # Insert new row
            pk_map[pk_value] = len(merged_rows)
            merged_rows.append(row)

    return merged_columns, merged_rows


async def _load_existing_csv(
    *,
    dataset: Any,
    dataset_registry: DatasetRegistry,
    lakefs_storage_service: Any,
    repo: str,
) -> Tuple[List[str], List[List[str]]]:
    """Load the latest version's CSV from lakeFS. Returns (columns, rows) or empty if none."""
    try:
        version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
        if version is None or not version.artifact_key:
            return [], []

        # Stored connector snapshots are read back through the branch/path layout.
        branch_name = (dataset.branch or "main").strip() or "main"
        object_prefix = dataset_ops._dataset_artifact_prefix(
            db_name=dataset.db_name, dataset_id=dataset.dataset_id, dataset_name=dataset.name,
        )
        object_key = f"{object_prefix}/source.csv"

        csv_bytes = await lakefs_storage_service.load_bytes(
            repo, f"{branch_name}/{object_key}",
        )
        if not csv_bytes:
            return [], []

        return _parse_csv_bytes(csv_bytes)
    except Exception as exc:
        logger.warning("Failed to load existing CSV for sync mode: %s", exc)
        return [], []


@trace_external_call("bff.data_connector_pipelining.start_pipelining_google_sheet")
async def start_pipelining_google_sheet(
    *,
    sheet_id: str,
    payload: Dict[str, Any],
    http_request: Request,
    google_sheets_service: GoogleSheetsService,
    connector_registry: ConnectorRegistry,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    objectify_job_queue: ObjectifyJobQueue,
    lineage_store: LineageStore,
) -> Dict[str, Any]:
    try:
        actor_user_id = (http_request.headers.get("X-User-ID") or "").strip() or None
        lakefs_storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)

        sanitized = sanitize_input(payload or {})
        db_name = str(sanitized.get("db_name") or "").strip()
        worksheet_name = str(sanitized.get("worksheet_name") or "").strip() or None
        api_key = sanitized.get("api_key")
        limit = int(sanitized.get("limit") or 25)
        limit = max(1, min(limit, 500))

        source = await connector_registry.get_source(source_type="google_sheets", source_id=sheet_id)
        if not source or not source.enabled:
            raise classified_http_exception(404, "Sheet is not registered", code=ErrorCode.RESOURCE_NOT_FOUND)

        mapping = await connector_registry.get_mapping(source_type="google_sheets", source_id=sheet_id)
        if not db_name and mapping and mapping.target_db_name:
            db_name = str(mapping.target_db_name)
        if not db_name:
            raise classified_http_exception(400, "db_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        db_name = validate_db_name(db_name)

        sheet_url = (source.config_json or {}).get("sheet_url")
        if not sheet_url:
            raise classified_http_exception(500, "Registered sheet missing URL", code=ErrorCode.CONNECTOR_ERROR)
        config = source.config_json or {}
        default_ws = config.get("worksheet_name")
        access_token = config.get("access_token")
        connection_id = config.get("connection_id")
        if connection_id:
            oauth_client = _build_google_oauth_client()
            _, refreshed_token = await _resolve_google_connection(
                connector_registry=connector_registry,
                oauth_client=oauth_client,
                connection_id=str(connection_id),
            )
            access_token = refreshed_token or access_token

        sheet_id_resolved, metadata, worksheet_title, _worksheet_sheet_id, values = await google_sheets_service.fetch_sheet_values(
            sheet_url,
            worksheet_name=worksheet_name or default_ws,
            api_key=api_key,
            access_token=access_token,
        )
        columns: list[str] = []
        rows: list[list[Any]] = []
        try:
            from data_connector.google_sheets.utils import normalize_sheet_data

            columns, rows = normalize_sheet_data(values)
        except Exception:
            logging.getLogger(__name__).warning("Exception fallback at bff/services/data_connector_pipelining_service.py:101", exc_info=True)
            columns = []
            rows = []

        sample_rows = rows[: max(1, limit)] if rows else []
        preview = {
            "sheet_id": sheet_id_resolved,
            "sheet_title": metadata.title,
            "worksheet_name": worksheet_title,
            "columns": columns,
            "sample_rows": sample_rows,
            "total_rows": len(rows),
        }

        tabular_analysis = await dataset_ops._compute_tabular_analysis_from_sample(
            {"columns": [{"name": col} for col in columns], "rows": sample_rows}
        )
        inferred_schema = tabular_analysis.get("columns") if isinstance(tabular_analysis, dict) else None
        if not isinstance(inferred_schema, list):
            inferred_schema = []
        schema_columns = dataset_ops._build_schema_columns(columns, inferred_schema)

        source_ref = f"google_sheets:{sheet_id_resolved}"
        resolved_branch = mapping.target_branch if mapping and mapping.target_branch else "main"
        dataset, created_dataset = await get_or_create_dataset_record(
            lookup=lambda: dataset_registry.get_dataset_by_source_ref(
                db_name=db_name,
                source_type="connector",
                source_ref=source_ref,
                branch=resolved_branch,
            ),
            create=lambda: dataset_registry.create_dataset(
                db_name=db_name,
                name=f"gsheet_{sheet_id_resolved}",
                description=f"Google Sheets sync: {sheet_url}",
                source_type="connector",
                source_ref=source_ref,
                schema_json={"columns": schema_columns},
                branch=resolved_branch,
            ),
            conflict_context=f"{db_name}/{source_ref}@{resolved_branch}",
        )

        if created_dataset:
            create_event = build_command_event(
                event_type="DATASET_CREATED",
                aggregate_type="Dataset",
                aggregate_id=dataset.dataset_id,
                data={"dataset_id": dataset.dataset_id, "db_name": db_name, "name": dataset.name},
                command_type="CREATE_DATASET",
            )
            create_event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
            try:
                await event_store.connect()
                await event_store.append_event(create_event)
            except Exception as e:
                logger.warning("Failed to append gsheet dataset create event: %s", e)

        repo = dataset_ops._resolve_lakefs_raw_repository()
        object_prefix = dataset_ops._dataset_artifact_prefix(
            db_name=db_name, dataset_id=dataset.dataset_id, dataset_name=dataset.name
        )
        object_key = f"{object_prefix}/source.csv"

        # -----------------------------------------------------------
        # Sync mode: SNAPSHOT (default), APPEND, or UPDATE
        # -----------------------------------------------------------
        import_mode = str(config.get("import_mode") or "SNAPSHOT").strip().upper()
        if import_mode not in {"SNAPSHOT", "APPEND", "UPDATE"}:
            import_mode = "SNAPSHOT"

        final_columns = columns
        final_rows = rows

        if import_mode in {"APPEND", "UPDATE"} and dataset:
            existing_columns, existing_rows = await _load_existing_csv(
                dataset=dataset,
                dataset_registry=dataset_registry,
                lakefs_storage_service=lakefs_storage_service,
                repo=repo,
            )
            if existing_columns or existing_rows:
                if import_mode == "APPEND":
                    final_columns, final_rows = _apply_append_mode(
                        existing_columns, existing_rows, columns, rows,
                    )
                    logger.info(
                        "APPEND mode: %d existing + %d new → %d total rows",
                        len(existing_rows), len(rows), len(final_rows),
                    )
                elif import_mode == "UPDATE":
                    pk_column = config.get("primary_key_column") or None
                    final_columns, final_rows = _apply_update_mode(
                        existing_columns, existing_rows, columns, rows,
                        primary_key_column=pk_column,
                    )
                    logger.info(
                        "UPDATE mode (pk=%s): %d existing + %d incoming → %d merged rows",
                        pk_column or "(first column)", len(existing_rows), len(rows), len(final_rows),
                    )

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        if final_columns:
            writer.writerow(final_columns)
        for row in final_rows:
            writer.writerow(row)

        branch_name = (dataset.branch or "main").strip() or "main"
        await dataset_ops._ensure_lakefs_branch_exists(
            lakefs_client=lakefs_client,
            repository=repo,
            branch=branch_name,
            source_branch="main",
        )
        await lakefs_storage_service.save_bytes(
            repo,
            f"{branch_name}/{object_key}",
            csv_buffer.getvalue().encode("utf-8"),
            content_type="text/csv",
        )
        commit_msg_mode = import_mode.lower()
        commit_id = await dataset_ops._commit_lakefs_with_predicate_fallback(
            lakefs_client=lakefs_client,
            lakefs_storage_service=lakefs_storage_service,
            repository=repo,
            branch=branch_name,
            message=f"Google Sheets {commit_msg_mode} {db_name}/{dataset.name}",
            metadata=dataset_ops._sanitize_s3_metadata(
                {
                    "dataset_id": dataset.dataset_id,
                    "db_name": db_name,
                    "dataset_name": dataset.name,
                    "source_type": "connector",
                    "source_ref": source_ref,
                }
            ),
            object_key=object_key,
        )
        artifact_key = build_s3_uri(repo, f"{commit_id}/{object_key}")

        objectify_job_id: Optional[str] = None
        if final_rows:
            version = await dataset_registry.add_version(
                dataset_id=dataset.dataset_id,
                lakefs_commit_id=commit_id,
                artifact_key=artifact_key,
                row_count=len(final_rows),
                sample_json={"columns": schema_columns, "rows": sample_rows},
                schema_json={"columns": schema_columns},
            )
            try:
                objectify_job_id = await dataset_ops._maybe_enqueue_objectify_job(
                    dataset=dataset,
                    version=version,
                    objectify_registry=objectify_registry,
                    job_queue=objectify_job_queue,
                    dataset_registry=dataset_registry,
                    actor_user_id=actor_user_id,
                )
            except Exception as exc:
                logger.warning("Failed to enqueue objectify job for connector dataset: %s", exc)

            event = build_command_event(
                event_type="DATASET_VERSION_CREATED",
                aggregate_type="Dataset",
                aggregate_id=dataset.dataset_id,
                data={
                    "dataset_id": dataset.dataset_id,
                    "db_name": db_name,
                    "name": dataset.name,
                    "lakefs_commit_id": version.lakefs_commit_id,
                    "artifact_key": artifact_key,
                },
                command_type="INGEST_DATASET_SNAPSHOT",
            )
            event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
            try:
                await event_store.connect()
                await event_store.append_event(event)
            except Exception as e:
                logger.warning("Failed to append gsheet dataset version event: %s", e)

        await lineage_store.record_link(
            from_node_id=lineage_store.node_artifact("connector", "google_sheets", str(sheet_id_resolved)),
            to_node_id=lineage_store.node_aggregate("Dataset", dataset.dataset_id),
            edge_type="connector_start_pipelining",
            db_name=db_name,
            edge_metadata={
                "db_name": db_name,
                "source_type": "google_sheets",
                "source_id": str(sheet_id_resolved),
                "dataset_id": dataset.dataset_id,
                "preview": preview,
            },
        )

        return {
            "status": "success",
            "message": "Start pipelining completed",
            "data": {
                "dataset": {
                    "dataset_id": dataset.dataset_id,
                    "db_name": dataset.db_name,
                    "name": dataset.name,
                    "schema_json": {"columns": schema_columns},
                    "source_type": dataset.source_type,
                    "source_ref": dataset.source_ref,
                },
                "sample": {"columns": schema_columns, "rows": sample_rows},
                "tabular_analysis": tabular_analysis,
                "objectify_job_id": objectify_job_id,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to start pipelining: %s", e)
        raise classified_http_exception(500, str(e), code=ErrorCode.CONNECTOR_ERROR) from e


def _validate_table_import_runtime_config(
    *,
    connector_kind: str,
    import_mode: str,
    import_config: Dict[str, Any],
) -> None:
    validate_resource_import_config(
        resource_kind="table_import",
        connector_kind=connector_kind,
        import_mode=import_mode,
        config=import_config,
    )


def _validate_file_import_runtime_config(
    *,
    connector_kind: str,
    import_mode: str,
    import_config: Dict[str, Any],
) -> None:
    validate_resource_import_config(
        resource_kind="file_import",
        connector_kind=connector_kind,
        import_mode=import_mode,
        config=import_config,
    )


def _validate_virtual_table_runtime_config(
    *,
    connector_kind: str,
    import_mode: str,
    import_config: Dict[str, Any],
) -> None:
    validate_resource_import_config(
        resource_kind="virtual_table",
        connector_kind=connector_kind,
        import_mode=import_mode,
        config=import_config,
        virtual_table_snapshot_error="virtual table execution supports only SNAPSHOT mode",
    )


async def start_pipelining_table_import(
    *,
    source: ConnectorSource,
    mapping: ConnectorMapping,
    google_sheets_service: GoogleSheetsService,
    connector_adapter_factory: Optional[ConnectorAdapterFactory],
    connector_registry: ConnectorRegistry,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    objectify_job_queue: ObjectifyJobQueue,
    actor_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    return await _start_pipelining_connector_import(
        source=source,
        mapping=mapping,
        google_sheets_service=google_sheets_service,
        connector_adapter_factory=connector_adapter_factory,
        connector_registry=connector_registry,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        actor_user_id=actor_user_id,
        import_config_key="table_import_config",
        execution_label="Table import execution completed",
        resource_kind="table import",
        config_validator=_validate_table_import_runtime_config,
    )


async def start_pipelining_file_import(
    *,
    source: ConnectorSource,
    mapping: ConnectorMapping,
    google_sheets_service: GoogleSheetsService,
    connector_adapter_factory: Optional[ConnectorAdapterFactory],
    connector_registry: ConnectorRegistry,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    objectify_job_queue: ObjectifyJobQueue,
    actor_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    return await _start_pipelining_connector_import(
        source=source,
        mapping=mapping,
        google_sheets_service=google_sheets_service,
        connector_adapter_factory=connector_adapter_factory,
        connector_registry=connector_registry,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        actor_user_id=actor_user_id,
        import_config_key="file_import_config",
        execution_label="File import execution completed",
        resource_kind="file import",
        config_validator=_validate_file_import_runtime_config,
    )


async def start_pipelining_virtual_table(
    *,
    source: ConnectorSource,
    mapping: ConnectorMapping,
    google_sheets_service: GoogleSheetsService,
    connector_adapter_factory: Optional[ConnectorAdapterFactory],
    connector_registry: ConnectorRegistry,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    objectify_job_queue: ObjectifyJobQueue,
    actor_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    return await _start_pipelining_connector_import(
        source=source,
        mapping=mapping,
        google_sheets_service=google_sheets_service,
        connector_adapter_factory=connector_adapter_factory,
        connector_registry=connector_registry,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        actor_user_id=actor_user_id,
        import_config_key="virtual_table_config",
        execution_label="Virtual table execution completed",
        resource_kind="virtual table",
        config_validator=_validate_virtual_table_runtime_config,
    )


async def _start_pipelining_connector_import(
    *,
    source: ConnectorSource,
    mapping: ConnectorMapping,
    google_sheets_service: GoogleSheetsService,
    connector_adapter_factory: Optional[ConnectorAdapterFactory],
    connector_registry: ConnectorRegistry,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    objectify_job_queue: ObjectifyJobQueue,
    actor_user_id: Optional[str],
    import_config_key: str,
    execution_label: str,
    resource_kind: str,
    config_validator: Any,
) -> Dict[str, Any]:
    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    adapter_factory = connector_adapter_factory or ConnectorAdapterFactory(google_sheets_service=google_sheets_service)
    adapter = adapter_factory.get_adapter(connector_kind)

    config, secrets = await resolve_source_runtime_credentials(
        connector_registry=connector_registry,
        source_type=source.source_type,
        source_config=dict(source.config_json or {}),
    )
    source_cfg = dict(source.config_json or {})
    import_config = source_cfg.get(import_config_key) if isinstance(source_cfg.get(import_config_key), dict) else {}
    import_mode = normalize_import_mode(source_cfg.get("import_mode"), mode_field_name="import_mode")
    config_validator(
        connector_kind=connector_kind,
        import_mode=import_mode,
        import_config=import_config,
    )

    sync_state = await connector_registry.get_sync_state(source_type=source.source_type, source_id=source.source_id)
    sync_state_json = dict(sync_state.sync_state_json or {}) if sync_state else {}

    if import_mode == "SNAPSHOT":
        extract = await adapter.snapshot_extract(
            config=config,
            secrets=secrets,
            import_config=import_config,
        )
    elif import_mode == "INCREMENTAL":
        extract = await adapter.incremental_extract(
            config=config,
            secrets=secrets,
            import_config=import_config,
            sync_state=sync_state_json,
        )
    elif import_mode in CDC_COMPAT_IMPORT_MODES:
        extract = await adapter.cdc_extract(
            config=config,
            secrets=secrets,
            import_config=import_config,
            sync_state=sync_state_json,
        )
    elif import_mode in {"APPEND", "UPDATE"}:
        # For connectors with no distinct append/update extraction, use snapshot extraction and ingest merge modes.
        extract = await adapter.snapshot_extract(
            config=config,
            secrets=secrets,
            import_config=import_config,
        )
    else:
        raise ValueError("import_mode must be one of SNAPSHOT, APPEND, UPDATE, INCREMENTAL, CDC, STREAMING")

    ingest_service = ConnectorIngestService(
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
    )
    branch_name = str(mapping.target_branch or "").strip() or "main"
    db_name = str(mapping.target_db_name or "").strip()
    if not db_name:
        raise ValueError("target_db_name is required")

    ingest_result = await ingest_service.ingest_rows(
        db_name=db_name,
        source_type=source.source_type,
        source_id=source.source_id,
        columns=list(extract.columns or []),
        rows=list(extract.rows or []),
        branch=branch_name,
        dataset_name=str(source_cfg.get("display_name") or f"{connector_kind}_{source.source_id}"),
        import_mode=import_mode,
        primary_key_column=(
            str(import_config.get("primaryKeyColumn") or import_config.get("primary_key_column") or "").strip() or None
        ),
        actor_user_id=actor_user_id,
        source_ref=f"{source.source_type}:{source.source_id}",
    )

    if extract.next_state:
        await connector_registry.upsert_sync_state_json(
            source_type=source.source_type,
            source_id=source.source_id,
            sync_state_json=dict(extract.next_state),
            merge=True,
        )

    return {
        "status": "success",
        "message": execution_label,
        "resourceKind": resource_kind,
        "data": {
            "dataset": ingest_result.get("dataset") if isinstance(ingest_result, dict) else {},
            "version": ingest_result.get("version") if isinstance(ingest_result, dict) else {},
            "objectify_job_id": (ingest_result or {}).get("objectify_job_id") if isinstance(ingest_result, dict) else None,
        },
    }
