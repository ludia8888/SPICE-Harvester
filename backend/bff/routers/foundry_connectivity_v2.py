import logging
from typing import Any, Dict
from uuid import NAMESPACE_URL, uuid4, uuid5

from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi.responses import JSONResponse

from bff.routers.data_connector_deps import (
    get_connector_registry,
    get_dataset_registry,
    get_google_sheets_service,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry,
)
from bff.services import data_connector_pipelining_service, data_connector_registration_service
from data_connector.google_sheets.service import GoogleSheetsService
from shared.dependencies.providers import LineageStoreDep
from shared.observability.tracing import trace_endpoint
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.connector_registry import ConnectorMapping, ConnectorRegistry, ConnectorSource
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/connectivity", tags=["Foundry Connectivity v2"])

_CONNECTION_RID_PREFIXES = (
    "ri.spice.main.connection.",
    "ri.foundry.main.connection.",
)
_TABLE_IMPORT_RID_PREFIXES = (
    "ri.spice.main.table-import.",
    "ri.foundry.main.table-import.",
)
_DATASET_RID_PREFIXES = (
    "ri.spice.main.dataset.",
    "ri.foundry.main.dataset.",
)
_TABLE_IMPORT_CONFIG_TYPES = {
    "databricksImportConfig",
    "jdbcImportConfig",
    "microsoftAccessImportConfig",
    "microsoftSqlServerImportConfig",
    "oracleImportConfig",
    "postgreSqlImportConfig",
    "snowflakeImportConfig",
}
_TABLE_IMPORT_MODES = {"SNAPSHOT", "APPEND", "UPDATE"}


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "errorCode": error_code,
            "errorName": error_name,
            "errorInstanceId": str(uuid4()),
            "parameters": parameters or {},
        },
    )


def _connection_id_from_rid(connection_rid: str) -> str | None:
    text = str(connection_rid or "").strip()
    if not text:
        return None
    for prefix in _CONNECTION_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    # Allow internal raw connection IDs for staged migration.
    if text.startswith("ri."):
        return None
    return text


def _connection_rid(connection_id: str) -> str:
    return f"ri.spice.main.connection.{connection_id}"


def _sheet_id_from_table_import_rid(table_import_rid: str) -> str | None:
    text = str(table_import_rid or "").strip()
    if not text:
        return None
    for prefix in _TABLE_IMPORT_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _table_import_rid(sheet_id: str) -> str:
    return f"ri.spice.main.table-import.{sheet_id}"


def _dataset_id_from_rid(dataset_rid: str) -> str | None:
    text = str(dataset_rid or "").strip()
    if not text:
        return None
    for prefix in _DATASET_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _dataset_rid(dataset_id: str) -> str:
    return f"ri.spice.main.dataset.{dataset_id}"


def _table_import_pipeline_id(*, connection_id: str, sheet_id: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"spice:table-import:{connection_id}:{sheet_id}"))


def _table_import_build_job_id(*, pipeline_id: str) -> str:
    return f"build-{pipeline_id}-{uuid4()}"


def _resolve_table_import_config(payload: dict[str, Any], *, sheet_url: str, worksheet_name: str | None) -> dict[str, Any]:
    config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    config_type = str(config.get("type") or "").strip()
    query = str(config.get("query") or "").strip()

    if config_type and config_type in _TABLE_IMPORT_CONFIG_TYPES and query:
        return {"type": config_type, "query": query}

    if query:
        return {"type": "jdbcImportConfig", "query": query}

    # Internal Google Sheets fallback while keeping a Foundry-shaped config payload.
    suffix = f"#{worksheet_name}" if worksheet_name else ""
    return {
        "type": "jdbcImportConfig",
        "query": f"SELECT * FROM EXTERNAL_SHEET('{sheet_url}{suffix}')",
    }


def _resolve_register_sheet_payload(
    payload: dict[str, Any],
    *,
    connection_id: str,
    dataset_db_name: str | None,
    dataset_branch: str | None,
) -> dict[str, Any]:
    source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
    destination = payload.get("destination") if isinstance(payload.get("destination"), dict) else {}

    sheet_url = str(
        source.get("sheetUrl")
        or source.get("url")
        or payload.get("sheetUrl")
        or payload.get("sourceUrl")
        or (payload.get("config") or {}).get("sheetUrl")
        or (payload.get("config") or {}).get("url")
        or ""
    ).strip()
    if not sheet_url:
        raise ValueError("sheetUrl is required (source.sheetUrl or config.sheetUrl)")

    worksheet_name = str(
        source.get("worksheetName")
        or source.get("worksheet")
        or payload.get("worksheetName")
        or (payload.get("config") or {}).get("worksheetName")
        or ""
    ).strip() or None

    polling_interval = payload.get("pollingIntervalSeconds")
    if polling_interval is None:
        polling_interval = source.get("pollingIntervalSeconds")
    if polling_interval is None:
        polling_interval = 300

    try:
        polling_interval = int(polling_interval)
    except (TypeError, ValueError) as exc:
        raise ValueError("pollingIntervalSeconds must be an integer") from exc

    branch_name = str(
        payload.get("branchName")
        or destination.get("branchName")
        or destination.get("branch")
        or dataset_branch
        or "main"
    ).strip() or "main"

    ontology = str(
        payload.get("ontology")
        or payload.get("databaseName")
        or destination.get("ontology")
        or destination.get("databaseName")
        or dataset_db_name
        or ""
    ).strip() or None

    object_type = str(
        payload.get("objectType")
        or payload.get("classLabel")
        or destination.get("objectType")
        or destination.get("classLabel")
        or ""
    ).strip() or None

    resolved: dict[str, Any] = {
        "sheet_url": sheet_url,
        "worksheet_name": worksheet_name,
        "polling_interval": polling_interval,
        "connection_id": connection_id,
        "api_key": source.get("apiKey") or payload.get("apiKey"),
        "branch": branch_name,
    }

    if ontology:
        resolved["database_name"] = ontology
    if object_type:
        resolved["class_label"] = object_type
        resolved["auto_import"] = True
    return resolved


async def _resolve_dataset_context(
    *,
    payload: dict[str, Any],
    dataset_registry: DatasetRegistry,
) -> tuple[str | None, str | None, str | None]:
    dataset_rid = str(payload.get("datasetRid") or "").strip() or None
    if not dataset_rid:
        return None, None, None

    dataset_id = _dataset_id_from_rid(dataset_rid)
    if not dataset_id:
        return dataset_rid, None, None

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if dataset is None:
        raise LookupError("datasetRid not found")

    resolved_dataset_rid = _dataset_rid(dataset.dataset_id)
    dataset_branch = str(dataset.branch or "").strip() or "main"
    dataset_db_name = str(dataset.db_name or "").strip() or None
    return resolved_dataset_rid, dataset_db_name, dataset_branch


async def _load_table_import_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    sheet_id: str,
) -> tuple[ConnectorSource | None, ConnectorMapping | None]:
    source = await connector_registry.get_source(source_type="google_sheets", source_id=sheet_id)
    if source is None:
        return None, None
    cfg = source.config_json or {}
    owner_connection = str(cfg.get("connection_id") or "").strip()
    if owner_connection and owner_connection != connection_id:
        return None, None
    mapping = await connector_registry.get_mapping(source_type="google_sheets", source_id=sheet_id)
    return source, mapping


async def _resolve_output_dataset_rid(
    *,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> str:
    cfg = source.config_json or {}
    configured_rid = str(cfg.get("dataset_rid") or "").strip()
    if configured_rid:
        return configured_rid

    dataset_id = _dataset_id_from_rid(str(cfg.get("dataset_id") or "").strip())
    if dataset_id:
        return _dataset_rid(dataset_id)

    target_db = str(mapping.target_db_name or "").strip() if mapping else ""
    target_branch = str(mapping.target_branch or "").strip() if mapping else ""
    if target_db:
        dataset = await dataset_registry.get_dataset_by_source_ref(
            db_name=target_db,
            source_type="connector",
            source_ref=f"google_sheets:{source.source_id}",
            branch=target_branch or "main",
        )
        if dataset is not None:
            return _dataset_rid(dataset.dataset_id)

    return _dataset_rid(f"connector-google-sheets-{source.source_id}")


async def _build_table_import_response(
    *,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> dict[str, Any]:
    cfg = source.config_json or {}
    dataset_rid = await _resolve_output_dataset_rid(
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
    )
    import_mode = str(cfg.get("import_mode") or "SNAPSHOT").strip().upper()
    if import_mode not in _TABLE_IMPORT_MODES:
        import_mode = "SNAPSHOT"

    table_import_config = cfg.get("table_import_config") if isinstance(cfg.get("table_import_config"), dict) else None
    if table_import_config is None:
        table_import_config = _resolve_table_import_config(
            {},
            sheet_url=str(cfg.get("sheet_url") or ""),
            worksheet_name=str(cfg.get("worksheet_name") or "").strip() or None,
        )

    display_name = str(cfg.get("display_name") or "").strip() or str(cfg.get("sheet_title") or "").strip() or f"Google Sheet {source.source_id}"

    return {
        "rid": _table_import_rid(source.source_id),
        "connectionRid": _connection_rid(connection_id),
        "datasetRid": dataset_rid,
        "branchName": str(mapping.target_branch or "").strip() if mapping else (str(cfg.get("branch_name") or "").strip() or None),
        "displayName": display_name,
        "importMode": import_mode,
        "allowSchemaChanges": bool(cfg.get("allow_schema_changes") or False),
        "config": table_import_config,
    }


async def _ensure_table_import_pipeline(
    *,
    pipeline_registry: PipelineRegistry,
    connection_id: str,
    sheet_id: str,
    mapping: ConnectorMapping,
) -> str:
    pipeline_id = _table_import_pipeline_id(connection_id=connection_id, sheet_id=sheet_id)
    existing = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if existing is not None:
        return pipeline_id

    db_name = str(mapping.target_db_name or "").strip() or "main"
    branch = str(mapping.target_branch or "").strip() or "main"
    pipeline_name = f"table_import_{connection_id}_{sheet_id}"
    await pipeline_registry.create_pipeline(
        pipeline_id=pipeline_id,
        db_name=db_name,
        name=pipeline_name,
        description=f"Foundry connectivity table import pipeline for {sheet_id}",
        pipeline_type="connector_table_import",
        location=f"connectivity/connections/{connection_id}/tableImports/{sheet_id}",
        status="active",
        branch=branch,
    )
    return pipeline_id


def _build_execute_run_output(
    *,
    connection_rid: str,
    table_import_rid: str,
    branch_name: str,
    requested_by: str,
    result_payload: dict[str, Any] | None = None,
    error_detail: str | None = None,
) -> dict[str, Any]:
    output: dict[str, Any] = {
        "connectionRid": connection_rid,
        "tableImportRid": table_import_rid,
        "branch": branch_name,
        "requested_by": requested_by,
        "outputs": [],
    }
    data = result_payload.get("data") if isinstance(result_payload, dict) else {}
    dataset_raw = data.get("dataset") if isinstance(data, dict) else {}
    dataset = dataset_raw if isinstance(dataset_raw, dict) else {}
    dataset_id = str(dataset.get("dataset_id") or "").strip()
    if dataset_id:
        output["outputs"] = [{"datasetRid": _dataset_rid(dataset_id)}]
    if error_detail:
        output["errors"] = [error_detail]
    return output


@router.post("/connections/{connectionRid}/tableImports")
@trace_endpoint("bff.foundry_v2_connectivity.create_table_import")
async def create_table_import_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    request: Request,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    *,
    lineage_store: LineageStoreDep,
):
    _ = request
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    connection = await connector_registry.get_source(source_type="google_sheets_connection", source_id=connection_id)
    if connection is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ConnectionNotFound",
            parameters={"connectionRid": connectionRid},
        )

    try:
        dataset_rid_input, dataset_db_name, dataset_branch = await _resolve_dataset_context(
            payload=payload,
            dataset_registry=dataset_registry,
        )
    except LookupError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": payload.get("datasetRid")},
        )

    try:
        sheet_data = _resolve_register_sheet_payload(
            payload,
            connection_id=connection_id,
            dataset_db_name=dataset_db_name,
            dataset_branch=dataset_branch,
        )
        registered = await data_connector_registration_service.register_google_sheet(
            sheet_data=sheet_data,
            google_sheets_service=google_sheets_service,
            connector_registry=connector_registry,
            dataset_registry=dataset_registry,
            lineage_store=lineage_store,
        )
        data = registered.get("data") if isinstance(registered, dict) else {}
        sheet_id = str(data.get("sheet_id") or "").strip()
        if not sheet_id:
            raise ValueError("sheet_id missing from registration response")

        dataset_payload = data.get("dataset") if isinstance(data.get("dataset"), dict) else {}
        resolved_dataset_rid = dataset_rid_input
        dataset_id = str(dataset_payload.get("dataset_id") or "").strip()
        if dataset_id:
            resolved_dataset_rid = _dataset_rid(dataset_id)
        if not resolved_dataset_rid:
            raise ValueError("datasetRid could not be resolved")

        import_mode = str(payload.get("importMode") or "SNAPSHOT").strip().upper()
        if import_mode not in _TABLE_IMPORT_MODES:
            raise ValueError("importMode must be one of SNAPSHOT, APPEND, UPDATE")

        display_name = str(payload.get("displayName") or "").strip() or f"Google Sheet Import {sheet_id}"
        allow_schema_changes = bool(payload.get("allowSchemaChanges") or False)

        table_import_config = _resolve_table_import_config(
            payload,
            sheet_url=str(sheet_data.get("sheet_url") or ""),
            worksheet_name=sheet_data.get("worksheet_name"),
        )

        source = await connector_registry.get_source(source_type="google_sheets", source_id=sheet_id)
        merged_cfg = dict(source.config_json if source else {})
        merged_cfg.update(
            {
                "connection_id": connection_id,
                "dataset_rid": resolved_dataset_rid,
                "display_name": display_name,
                "import_mode": import_mode,
                "allow_schema_changes": allow_schema_changes,
                "table_import_config": table_import_config,
                "branch_name": str(sheet_data.get("branch") or "main"),
            }
        )
        await connector_registry.upsert_source(
            source_type="google_sheets",
            source_id=sheet_id,
            enabled=True,
            config_json=merged_cfg,
        )

        final_source, final_mapping = await _load_table_import_source(
            connector_registry=connector_registry,
            connection_id=connection_id,
            sheet_id=sheet_id,
        )
        if final_source is None:
            raise ValueError("table import source could not be resolved")

        return await _build_table_import_response(
            connection_id=connection_id,
            source=final_source,
            mapping=final_mapping,
            dataset_registry=dataset_registry,
        )
    except ValueError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )
    except Exception as exc:
        logger.error("Failed to create connectivity table import: %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to create table import"},
        )


@router.get("/connections/{connectionRid}/tableImports/{tableImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.get_table_import")
async def get_table_import_v2(
    connectionRid: str,
    tableImportRid: str,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    sheet_id = _sheet_id_from_table_import_rid(tableImportRid)
    if not sheet_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "tableImportRid is invalid"},
        )

    source, mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        sheet_id=sheet_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    return await _build_table_import_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
    )


@router.get("/connections/{connectionRid}/tableImports")
@trace_endpoint("bff.foundry_v2_connectivity.list_table_imports")
async def list_table_imports_v2(
    connectionRid: str,
    pageSize: int | None = Query(default=None),
    pageToken: str | None = Query(default=None),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    connection = await connector_registry.get_source(source_type="google_sheets_connection", source_id=connection_id)
    if connection is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ConnectionNotFound",
            parameters={"connectionRid": connectionRid},
        )

    if pageSize is None:
        page_size = 100
    else:
        try:
            page_size = int(pageSize)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageSize must be an integer"},
            )
        if page_size <= 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageSize must be > 0"},
            )

    offset = 0
    if pageToken:
        try:
            offset = int(pageToken)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )
        if offset < 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )

    sources = await connector_registry.list_sources(source_type="google_sheets", enabled=True, limit=5000)
    owned_sources = [
        src
        for src in sources
        if str((src.config_json or {}).get("connection_id") or "").strip() == connection_id
    ]
    owned_sources.sort(key=lambda src: str(src.source_id))

    page_items = owned_sources[offset : offset + page_size]
    data: list[dict[str, Any]] = []
    for src in page_items:
        mapping = await connector_registry.get_mapping(source_type="google_sheets", source_id=src.source_id)
        data.append(
            await _build_table_import_response(
                connection_id=connection_id,
                source=src,
                mapping=mapping,
                dataset_registry=dataset_registry,
            )
        )

    next_token = offset + len(page_items)
    return {
        "data": data,
        "nextPageToken": str(next_token) if next_token < len(owned_sources) else None,
    }


@router.put("/connections/{connectionRid}/tableImports/{tableImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.replace_table_import")
async def replace_table_import_v2(
    connectionRid: str,
    tableImportRid: str,
    payload: Dict[str, Any],
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    sheet_id = _sheet_id_from_table_import_rid(tableImportRid)
    if not sheet_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "tableImportRid is invalid"},
        )

    source, mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        sheet_id=sheet_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    cfg = dict(source.config_json or {})
    import_mode = str(payload.get("importMode") or cfg.get("import_mode") or "SNAPSHOT").strip().upper()
    if import_mode not in _TABLE_IMPORT_MODES:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "importMode must be one of SNAPSHOT, APPEND, UPDATE"},
        )

    display_name = str(payload.get("displayName") or cfg.get("display_name") or "").strip() or f"Google Sheet Import {sheet_id}"
    allow_schema_changes = bool(
        payload.get("allowSchemaChanges")
        if payload.get("allowSchemaChanges") is not None
        else cfg.get("allow_schema_changes")
    )

    table_import_config = _resolve_table_import_config(
        payload,
        sheet_url=str(cfg.get("sheet_url") or ""),
        worksheet_name=str(cfg.get("worksheet_name") or "").strip() or None,
    )

    cfg.update(
        {
            "display_name": display_name,
            "import_mode": import_mode,
            "allow_schema_changes": allow_schema_changes,
            "table_import_config": table_import_config,
            "connection_id": connection_id,
        }
    )
    await connector_registry.upsert_source(
        source_type="google_sheets",
        source_id=sheet_id,
        enabled=source.enabled,
        config_json=cfg,
    )

    refreshed_source, refreshed_mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        sheet_id=sheet_id,
    )
    if refreshed_source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    return await _build_table_import_response(
        connection_id=connection_id,
        source=refreshed_source,
        mapping=refreshed_mapping,
        dataset_registry=dataset_registry,
    )


@router.delete("/connections/{connectionRid}/tableImports/{tableImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.delete_table_import")
async def delete_table_import_v2(
    connectionRid: str,
    tableImportRid: str,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
):
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    sheet_id = _sheet_id_from_table_import_rid(tableImportRid)
    if not sheet_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "tableImportRid is invalid"},
        )

    source, _mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        sheet_id=sheet_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    await connector_registry.set_source_enabled(
        source_type="google_sheets",
        source_id=sheet_id,
        enabled=False,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections/{connectionRid}/tableImports/{tableImportRid}/execute")
@trace_endpoint("bff.foundry_v2_connectivity.execute_table_import")
async def execute_table_import_v2(
    connectionRid: str,
    tableImportRid: str,
    request: Request,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    *,
    lineage_store: LineageStoreDep,
):
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    sheet_id = _sheet_id_from_table_import_rid(tableImportRid)
    if not sheet_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "tableImportRid is invalid"},
        )

    source, mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        sheet_id=sheet_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    if mapping is None or not str(mapping.target_db_name or "").strip():
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name="TableImportNotReady",
            parameters={"message": "table import mapping is incomplete; target ontology is required"},
        )

    branch_name = str(mapping.target_branch or "").strip() or "main"
    requested_by = str(request.headers.get("X-User-ID") or "").strip() or "system"
    connection_rid = _connection_rid(connection_id)
    table_import_rid = _table_import_rid(sheet_id)

    try:
        pipeline_id = await _ensure_table_import_pipeline(
            pipeline_registry=pipeline_registry,
            connection_id=connection_id,
            sheet_id=sheet_id,
            mapping=mapping,
        )
        job_id = _table_import_build_job_id(pipeline_id=pipeline_id)
        started_at = utcnow()

        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="RUNNING",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                table_import_rid=table_import_rid,
                branch_name=branch_name,
                requested_by=requested_by,
            ),
            started_at=started_at,
        )

        result_payload = await data_connector_pipelining_service.start_pipelining_google_sheet(
            sheet_id=sheet_id,
            payload={},
            http_request=request,
            google_sheets_service=google_sheets_service,
            connector_registry=connector_registry,
            pipeline_registry=pipeline_registry,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            objectify_job_queue=objectify_job_queue,
            lineage_store=lineage_store,
        )

        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="SUCCESS",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                table_import_rid=table_import_rid,
                branch_name=branch_name,
                requested_by=requested_by,
                result_payload=result_payload if isinstance(result_payload, dict) else None,
            ),
            started_at=started_at,
            finished_at=utcnow(),
        )
    except Exception as exc:
        logger.error("Failed to execute table import %s: %s", tableImportRid, exc)
        try:
            failure_job_id = locals().get("job_id")
            failure_pipeline_id = locals().get("pipeline_id")
            failure_started_at = locals().get("started_at")
            if failure_job_id and failure_pipeline_id:
                await pipeline_registry.record_run(
                    pipeline_id=str(failure_pipeline_id),
                    job_id=str(failure_job_id),
                    mode="build",
                    status="FAILED",
                    output_json=_build_execute_run_output(
                        connection_rid=connection_rid,
                        table_import_rid=table_import_rid,
                        branch_name=branch_name,
                        requested_by=requested_by,
                        error_detail=str(exc),
                    ),
                    started_at=failure_started_at if failure_started_at is not None else utcnow(),
                    finished_at=utcnow(),
                )
        except Exception as record_exc:
            logger.warning("Failed to record execute_table_import failure build run: %s", record_exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to execute table import"},
        )

    return f"ri.spice.main.build.{job_id}"


# ---------------------------------------------------------------------------
# Connection CRUD — Foundry Connectivity Connections API v2
# ---------------------------------------------------------------------------


def _connection_response(source: ConnectorSource) -> dict[str, Any]:
    """Build Foundry-shaped Connection response from a ConnectorSource."""
    cfg = source.config_json or {}

    display_name = (
        str(cfg.get("display_name") or "").strip()
        or str(cfg.get("name") or "").strip()
        or str(cfg.get("account_email") or "").strip()
        or f"Connection {source.source_id}"
    )

    conn_status = "CONNECTED"
    if not source.enabled:
        conn_status = "DISCONNECTED"
    elif cfg.get("status"):
        text = str(cfg["status"]).strip().upper()
        if text in {"ERROR", "FAILED"}:
            conn_status = "ERROR"
        elif text in {"DISCONNECTED", "DISABLED"}:
            conn_status = "DISCONNECTED"

    config_type = str(cfg.get("type") or "GoogleSheetsConnectionConfig").strip()

    configuration: dict[str, Any] = {"type": config_type}
    # Include non-sensitive config fields
    sheet_url = cfg.get("sheet_url") or cfg.get("sheetUrl")
    account_email = cfg.get("account_email") or cfg.get("accountEmail") or cfg.get("accountemail")
    if sheet_url:
        configuration["sheetUrl"] = str(sheet_url)
    if account_email:
        configuration["accountEmail"] = str(account_email)
    if cfg.get("scopes"):
        configuration["scopes"] = cfg["scopes"]

    return {
        "rid": _connection_rid(source.source_id),
        "displayName": display_name,
        "configuration": configuration,
        "status": conn_status,
        "createdTime": _iso_timestamp(source.created_at),
        "updatedTime": _iso_timestamp(source.updated_at),
    }


def _iso_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


@router.post("/connections")
@trace_endpoint("bff.foundry_v2_connectivity.create_connection")
async def create_connection_v2(
    payload: Dict[str, Any],
    request: Request,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> JSONResponse:
    """POST /v2/connectivity/connections — Create a connection."""
    _ = request
    display_name = str(payload.get("displayName") or payload.get("name") or "").strip()
    if not display_name:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "displayName is required"},
        )

    configuration = payload.get("configuration") if isinstance(payload.get("configuration"), dict) else {}
    config_type = str(configuration.get("type") or "GoogleSheetsConnectionConfig").strip()

    connection_id = str(uuid4())

    config_json: dict[str, Any] = {
        "display_name": display_name,
        "type": config_type,
        "status": "CONNECTED",
    }
    # Forward non-sensitive configuration fields in snake_case for internal consumers.
    if configuration.get("accountEmail"):
        config_json["account_email"] = configuration["accountEmail"]
    if configuration.get("sheetUrl"):
        config_json["sheet_url"] = configuration["sheetUrl"]
    if configuration.get("scopes"):
        config_json["scopes"] = configuration["scopes"]
    if configuration.get("serviceAccountJson"):
        config_json["service_account_json"] = configuration["serviceAccountJson"]

    # OAuth credentials
    if configuration.get("accessToken"):
        config_json["access_token"] = configuration["accessToken"]
    if configuration.get("refreshToken"):
        config_json["refresh_token"] = configuration["refreshToken"]

    try:
        await connector_registry.upsert_source(
            source_type="google_sheets_connection",
            source_id=connection_id,
            enabled=True,
            config_json=config_json,
        )
    except Exception as exc:
        logger.error("Failed to create connection: %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="ConnectionCreationFailed",
            parameters={"message": "Failed to create connection"},
        )

    created = await connector_registry.get_source(
        source_type="google_sheets_connection", source_id=connection_id,
    )
    if created is None:
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="ConnectionCreationFailed",
            parameters={"message": "Connection created but could not be retrieved"},
        )

    return JSONResponse(status_code=status.HTTP_201_CREATED, content=_connection_response(created))


@router.get("/connections/{connectionRid}")
@trace_endpoint("bff.foundry_v2_connectivity.get_connection")
async def get_connection_v2(
    connectionRid: str,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
):
    """GET /v2/connectivity/connections/{connectionRid} — Get a connection."""
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    connection = await connector_registry.get_source(
        source_type="google_sheets_connection", source_id=connection_id,
    )
    if connection is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ConnectionNotFound",
            parameters={"connectionRid": connectionRid},
        )

    return _connection_response(connection)


@router.get("/connections")
@trace_endpoint("bff.foundry_v2_connectivity.list_connections")
async def list_connections_v2(
    pageSize: int = Query(default=100, ge=1, le=500),
    pageToken: str | None = Query(default=None),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
):
    """GET /v2/connectivity/connections — List connections."""
    offset = 0
    if pageToken:
        try:
            offset = int(pageToken)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )
        if offset < 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )

    all_connections = await connector_registry.list_sources(
        source_type="google_sheets_connection", enabled=True, limit=5000,
    )
    all_connections.sort(key=lambda src: str(src.source_id))

    page_items = all_connections[offset: offset + pageSize]
    data = [_connection_response(conn) for conn in page_items]

    next_token = offset + len(page_items)
    return {
        "data": data,
        "nextPageToken": str(next_token) if next_token < len(all_connections) else None,
    }


@router.delete("/connections/{connectionRid}")
@trace_endpoint("bff.foundry_v2_connectivity.delete_connection")
async def delete_connection_v2(
    connectionRid: str,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
):
    """DELETE /v2/connectivity/connections/{connectionRid} — Delete connection."""
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    connection = await connector_registry.get_source(
        source_type="google_sheets_connection", source_id=connection_id,
    )
    if connection is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ConnectionNotFound",
            parameters={"connectionRid": connectionRid},
        )

    await connector_registry.set_source_enabled(
        source_type="google_sheets_connection",
        source_id=connection_id,
        enabled=False,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections/{connectionRid}/test")
@trace_endpoint("bff.foundry_v2_connectivity.test_connection")
async def test_connection_v2(
    connectionRid: str,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
):
    """POST /v2/connectivity/connections/{connectionRid}/test — Test a connection."""
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    connection = await connector_registry.get_source(
        source_type="google_sheets_connection", source_id=connection_id,
    )
    if connection is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ConnectionNotFound",
            parameters={"connectionRid": connectionRid},
        )

    cfg = connection.config_json or {}
    access_token = cfg.get("access_token")
    api_key = cfg.get("api_key")

    # If connection has an OAuth connection_id, try refreshing token
    inner_connection_id = cfg.get("connection_id")
    if inner_connection_id:
        try:
            from bff.routers.data_connector_ops import _build_google_oauth_client, _resolve_google_connection
            oauth_client = _build_google_oauth_client()
            _, refreshed_token = await _resolve_google_connection(
                connector_registry=connector_registry,
                oauth_client=oauth_client,
                connection_id=str(inner_connection_id),
            )
            if refreshed_token:
                access_token = refreshed_token
        except Exception:
            pass

    test_result: dict[str, Any] = {
        "connectionRid": _connection_rid(connection_id),
        "status": "SUCCEEDED",
        "message": "Connection is healthy",
    }

    try:
        # Try to ping Google Sheets API with current credentials
        if access_token or api_key:
            # Simple metadata fetch to verify credentials work
            sheet_url = cfg.get("sheetUrl") or cfg.get("sheet_url")
            if sheet_url:
                # Trigger metadata fetch via internal method to verify credentials
                from data_connector.google_sheets.utils import extract_sheet_id as _extract_sid
                await google_sheets_service._get_sheet_metadata(
                    _extract_sid(str(sheet_url)),
                    api_key=api_key,
                    access_token=access_token,
                )
            else:
                # No sheet URL to test against — credentials present but untestable
                test_result["message"] = "Connection credentials present (no test URL configured)"
        else:
            test_result["status"] = "WARNING"
            test_result["message"] = "No credentials configured"
    except Exception as exc:
        test_result["status"] = "FAILED"
        test_result["message"] = f"Connection test failed: {str(exc)[:200]}"

        # Update connection status in registry
        try:
            merged_cfg = dict(cfg)
            merged_cfg["status"] = "ERROR"
            await connector_registry.upsert_source(
                source_type="google_sheets_connection",
                source_id=connection_id,
                enabled=connection.enabled,
                config_json=merged_cfg,
            )
        except Exception:
            pass

    return test_result
