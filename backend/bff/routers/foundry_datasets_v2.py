"""Foundry Datasets API v2 — Palantir-compatible dataset lifecycle.

Implements the Foundry v2 Datasets REST surface on top of existing
DatasetRegistry (Postgres), LakeFSClient (versioned storage), and
LakeFSStorageService (S3-compatible object storage).

Endpoints
---------
Dataset CRUD:
  POST   /v2/datasets                                      Create dataset
  GET    /v2/datasets/{datasetRid}                         Get dataset
  GET    /v2/datasets/{datasetRid}/getSchema               Get dataset schema (preview)
  POST   /v2/datasets/getSchemaBatch                       Get dataset schemas batch (preview)
  PUT    /v2/datasets/{datasetRid}/putSchema               Put dataset schema (preview)

Branch management:
  GET    /v2/datasets/{datasetRid}/branches                List branches
  POST   /v2/datasets/{datasetRid}/branches                Create branch
  GET    /v2/datasets/{datasetRid}/branches/{branchName}   Get branch
  DELETE /v2/datasets/{datasetRid}/branches/{branchName}   Delete branch

Transactions:
  POST   /v2/datasets/{datasetRid}/transactions                          Create transaction
  GET    /v2/datasets/{datasetRid}/transactions                          List transactions
  GET    /v2/datasets/{datasetRid}/transactions/{transactionRid}         Get transaction
  POST   /v2/datasets/{datasetRid}/transactions/{transactionRid}/commit  Commit transaction
  POST   /v2/datasets/{datasetRid}/transactions/{transactionRid}/abort   Abort transaction

Files & table read:
  GET    /v2/datasets/{datasetRid}/files                                   List files
  GET    /v2/datasets/{datasetRid}/files/{filePath:path}                    Get file metadata
  GET    /v2/datasets/{datasetRid}/files/{filePath:path}/content            Get file content
  POST   /v2/datasets/{datasetRid}/files/{filePath:path}/upload             Upload file
  GET    /v2/datasets/{datasetRid}/readTable                                Read table rows
"""

import csv
import io
import logging
import mimetypes
from typing import Any, Dict, List, Optional

from uuid import NAMESPACE_URL, uuid4, uuid5

from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi.responses import JSONResponse

from bff.routers.data_connector_deps import get_dataset_registry, get_pipeline_registry
from bff.routers.pipeline_datasets_ops_lakefs import _resolve_lakefs_raw_repository, _ensure_lakefs_branch_exists
from shared.foundry.auth import require_scopes
from shared.foundry.errors import foundry_error
from shared.foundry.rids import build_rid, parse_rid
from shared.observability.tracing import trace_endpoint
from shared.security.input_sanitizer import SecurityViolationError, validate_branch_name, validate_db_name
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.lakefs_client import (
    LakeFSClient,
    LakeFSConflictError,
    LakeFSError,
    LakeFSNotFoundError,
)
from shared.utils.s3_uri import parse_s3_uri
from shared.utils.time_utils import utcnow
from shared.utils.foundry_page_token import decode_offset_page_token, encode_offset_page_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/datasets", tags=["Foundry Datasets v2"])

_DATASETS_JSON_EXCEPTIONS = (ValueError, TypeError)
_DATASETS_HANDLED_EXCEPTIONS = (
    RuntimeError,
    LookupError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    OSError,
    AssertionError,
)


def _datasets_invalid_json_response() -> JSONResponse:
    return foundry_error(
        400,
        error_code="INVALID_ARGUMENT",
        error_name="InvalidArgument",
        parameters={"message": "Invalid JSON body"},
    )


def _datasets_internal_error_response(
    *,
    exc: Exception,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
    log_message: str | None = None,
) -> JSONResponse:
    if log_message:
        logger.error("%s: %s", log_message, exc)
    payload: Dict[str, Any] = dict(parameters or {})
    payload.setdefault("message", str(exc))
    return foundry_error(
        500,
        error_code="INTERNAL",
        error_name=error_name,
        parameters=payload,
    )

# ---------------------------------------------------------------------------
# RID helpers
# ---------------------------------------------------------------------------

_DATASET_RID_PREFIXES = (
    "ri.spice.main.dataset.",
    "ri.foundry.main.dataset.",
)
_FOLDER_RID_PREFIXES = (
    "ri.spice.main.folder.",
    "ri.foundry.main.folder.",
)
_TRANSACTION_RID_PREFIXES = (
    "ri.spice.main.transaction.",
    "ri.foundry.main.transaction.",
)
_CSV_PREVIEW_ROW_LIMIT = 100


def _dataset_id_from_rid(dataset_rid: str) -> str | None:
    text = str(dataset_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "dataset":
        return None
    return parsed


def _dataset_rid(dataset_id: str) -> str:
    return build_rid("dataset", dataset_id)


def _folder_rid(db_name: str) -> str:
    return build_rid("folder", db_name)


def _db_name_from_folder_rid(folder_rid: str) -> str | None:
    text = str(folder_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "folder":
        return None
    return parsed


def _transaction_rid(transaction_id: str) -> str:
    return build_rid("transaction", transaction_id)


def _transaction_id_from_rid(txn_rid: str) -> str | None:
    text = str(txn_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "transaction":
        return None
    return parsed


def _file_rid(*, dataset_id: str, file_path: str) -> str:
    normalized_path = str(file_path or "").strip().lstrip("/")
    token = str(uuid5(NAMESPACE_URL, f"foundry:file:{dataset_id}:{normalized_path}"))
    return build_rid("file", token)


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def _dataset_response(dataset: Any) -> Dict[str, Any]:
    return {
        "rid": _dataset_rid(dataset.dataset_id),
        "name": dataset.name,
        "parentFolderRid": _folder_rid(dataset.db_name),
    }


def _schema_response(schema_json: Dict[str, Any]) -> Dict[str, Any]:
    columns = schema_json.get("columns") if isinstance(schema_json, dict) else []
    if not isinstance(columns, list):
        columns = []
    field_list = []
    for col in columns:
        if isinstance(col, dict):
            field_list.append({
                "fieldPath": col.get("name", ""),
                "type": {"type": col.get("type", "string")},
            })
    return {"fieldSchemaList": field_list}


def _branch_response(
    *,
    branch_name: str,
    dataset_rid: str,
    commit_id: str | None = None,
) -> Dict[str, Any]:
    return {
        "branchName": branch_name,
        "datasetRid": dataset_rid,
        "transactionRid": None,
        "latestCommitId": commit_id,
    }


def _transaction_response(txn: Any) -> Dict[str, Any]:
    return {
        "rid": _transaction_rid(txn.transaction_id),
        "datasetRid": None,
        "transactionType": "APPEND",
        "status": _map_txn_status(txn.status),
        "createdTime": txn.created_at.isoformat() if txn.created_at else None,
        "closedTime": (txn.committed_at or txn.aborted_at or "").isoformat()
        if (txn.committed_at or txn.aborted_at)
        else None,
    }


def _map_txn_status(raw: str | None) -> str:
    raw = str(raw or "").strip().upper()
    return {
        "OPEN": "OPEN",
        "COMMITTED": "COMMITTED",
        "ABORTED": "ABORTED",
    }.get(raw, "OPEN")


def _dataset_object_prefix(dataset: Any) -> str:
    return f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/"


def _normalize_dataset_object_key(dataset: Any, object_key: str) -> str:
    raw_key = str(object_key or "").strip()
    parsed = parse_s3_uri(raw_key)
    if parsed:
        _, parsed_key = parsed
        key = parsed_key.lstrip("/")
        branch_prefix = f"{(dataset.branch or 'master').strip('/')}/"
        if key.startswith(branch_prefix):
            key = key[len(branch_prefix):]
    else:
        key = raw_key.lstrip("/")
    prefix = _dataset_object_prefix(dataset)
    if not key:
        return f"{prefix}source.csv"
    found_prefix_at = key.find(prefix)
    if found_prefix_at >= 0:
        key = key[found_prefix_at:]
    if key.startswith(prefix):
        return key
    return f"{prefix}{key}"


def _artifact_s3_uri(*, repository: str, branch: str, object_key: str | None) -> str | None:
    normalized_key = str(object_key or "").strip().lstrip("/")
    if not normalized_key:
        return None
    normalized_branch = str(branch or "master").strip().strip("/") or "master"
    return f"s3://{repository}/{normalized_branch}/{normalized_key}"


def _normalized_csv_headers(raw_headers: List[str]) -> List[str]:
    headers: list[str] = []
    used: set[str] = set()
    for idx, raw in enumerate(raw_headers):
        base = str(raw or "").strip() or f"column_{idx + 1}"
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        headers.append(candidate)
        used.add(candidate)
    return headers


def _build_csv_sample_and_schema(csv_bytes: bytes) -> tuple[Dict[str, Any], Dict[str, Any], int]:
    decoded = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(decoded))
    all_rows = list(reader)
    if not all_rows:
        return {"columns": [], "rows": []}, {"columns": []}, 0

    headers = _normalized_csv_headers([str(cell) for cell in all_rows[0]])
    columns = [{"name": header, "type": "string"} for header in headers]
    sample_rows: list[list[str | None]] = []
    row_count = 0

    for raw_row in all_rows[1:]:
        row = [str(value) for value in raw_row]
        if len(row) < len(headers):
            row.extend([None] * (len(headers) - len(row)))
        elif len(row) > len(headers):
            row = row[: len(headers)]
        row_count += 1
        if len(sample_rows) < _CSV_PREVIEW_ROW_LIMIT:
            sample_rows.append(row)

    sample_json = {"columns": columns, "rows": sample_rows}
    schema_json = {"columns": columns}
    return sample_json, schema_json, row_count


def _default_sample_and_schema(dataset: Any) -> tuple[Dict[str, Any], Dict[str, Any], int]:
    raw_schema = dataset.schema_json if isinstance(dataset.schema_json, dict) else {}
    raw_columns = raw_schema.get("columns") if isinstance(raw_schema.get("columns"), list) else []
    columns = [col for col in raw_columns if isinstance(col, dict)]
    schema_json = {"columns": columns}
    sample_json = {"columns": columns, "rows": []}
    return sample_json, schema_json, 0


async def _extract_commit_preview(
    *,
    dataset: Any,
    branch: str,
    repository: str,
    lakefs_client: LakeFSClient,
    lakefs_storage_service: Any,
) -> tuple[str | None, Dict[str, Any], Dict[str, Any], int]:
    dataset_prefix = _dataset_object_prefix(dataset)
    candidate_keys = [_normalize_dataset_object_key(dataset, "source.csv")]

    try:
        objects = await lakefs_client.list_objects(
            repository=repository,
            ref=branch,
            prefix=dataset_prefix,
            amount=200,
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        logger.warning("Failed to list dataset objects for transaction preview: %s", exc)
        objects = []

    for obj in objects:
        if not isinstance(obj, dict):
            continue
        object_path = str(obj.get("path") or "").strip()
        if not object_path or not object_path.lower().endswith(".csv"):
            continue
        candidate_keys.append(_normalize_dataset_object_key(dataset, object_path))

    for key in dict.fromkeys(candidate_keys):
        try:
            csv_bytes = await lakefs_storage_service.load_bytes(repository, f"{branch}/{key}")
        except (FileNotFoundError, LakeFSNotFoundError):
            continue
        except _DATASETS_HANDLED_EXCEPTIONS as exc:
            logger.warning("Failed to load candidate CSV object during commit preview (%s): %s", key, exc)
            continue

        sample_json, schema_json, row_count = _build_csv_sample_and_schema(csv_bytes)
        return key, sample_json, schema_json, row_count

    sample_json, schema_json, row_count = _default_sample_and_schema(dataset)
    return None, sample_json, schema_json, row_count


# ---------------------------------------------------------------------------
# Dataset CRUD
# ---------------------------------------------------------------------------

@router.post(
    "",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.create_dataset")
async def create_dataset_v2(
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-write"],
    ),
) -> JSONResponse:
    """POST /v2/datasets — Create a new dataset."""
    try:
        body = await request.json()
    except _DATASETS_JSON_EXCEPTIONS:
        return _datasets_invalid_json_response()

    name = str(body.get("name") or "").strip()
    if not name:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "name is required"},
        )

    parent_folder_rid = str(body.get("parentFolderRid") or "").strip()
    db_name = _db_name_from_folder_rid(parent_folder_rid) if parent_folder_rid else None
    if not db_name:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidFolderRid",
            parameters={"parentFolderRid": parent_folder_rid},
        )

    try:
        db_name = validate_db_name(db_name)
    except SecurityViolationError as exc:
        logger.info("Invalid folder RID/db name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidFolderRid",
            parameters={"parentFolderRid": parent_folder_rid},
        )

    branch = "master"
    description = body.get("description")

    try:
        dataset = await dataset_registry.create_dataset(
            db_name=db_name,
            name=name,
            description=description,
            source_type="foundry_api",
            branch=branch,
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="DatasetCreationFailed",
            log_message="Failed to create dataset",
        )

    return JSONResponse(content=_dataset_response(dataset))


@router.get(
    "/{datasetRid}",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.get_dataset")
async def get_dataset_v2(
    datasetRid: str,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid} — Get a dataset."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    return JSONResponse(content=_dataset_response(dataset))


# ---------------------------------------------------------------------------
# Schema (preview)
# ---------------------------------------------------------------------------

@router.get(
    "/{datasetRid}/getSchema",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.get_dataset_schema")
async def get_dataset_schema_v2(
    datasetRid: str,
    preview: bool = Query(..., alias="preview"),
    branchName: str = Query(default="master", alias="branchName"),
    endTransactionRid: str = Query(default="", alias="endTransactionRid"),
    versionId: str = Query(default="", alias="versionId"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/getSchema — Get dataset schema (preview)."""
    if preview is not True:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="ApiFeaturePreviewUsageOnly",
            parameters={},
        )

    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    normalized_branch = str(branchName or "").strip() or "master"
    try:
        validate_branch_name(normalized_branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": normalized_branch},
        )

    # Best-effort validation for transaction/version selectors. Current implementation always
    # returns the latest stored schema, independent of transaction or version selectors.
    if endTransactionRid:
        if not _transaction_id_from_rid(endTransactionRid):
            return foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidTransactionRid",
                parameters={"endTransactionRid": endTransactionRid},
            )
    _ = versionId  # Placeholder: reserved for future version-specific schema materialization.

    schema_json = dataset.schema_json if isinstance(dataset.schema_json, dict) else {}
    if not schema_json or not schema_json.get("columns"):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="SchemaNotFound",
            parameters={"datasetRid": datasetRid},
        )

    return JSONResponse(content={"schema": _schema_response(schema_json)})


@router.put(
    "/{datasetRid}/putSchema",
    status_code=status.HTTP_204_NO_CONTENT,
)
@trace_endpoint("foundry_datasets_v2.put_dataset_schema")
async def put_dataset_schema_v2(
    datasetRid: str,
    request: Request,
    preview: bool = Query(..., alias="preview"),
    branchName: str = Query(default="master", alias="branchName"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-write"],
    ),
) -> Response:
    """PUT /v2/datasets/{datasetRid}/putSchema — Put dataset schema (preview)."""
    if preview is not True:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="ApiFeaturePreviewUsageOnly",
            parameters={},
        )

    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    try:
        body = await request.json()
    except _DATASETS_JSON_EXCEPTIONS:
        return _datasets_invalid_json_response()

    normalized_branch = str(branchName or "master").strip() or "master"
    try:
        validate_branch_name(normalized_branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": normalized_branch},
        )

    field_list = body.get("fieldSchemaList") if isinstance(body, dict) else None
    if not isinstance(field_list, list):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "fieldSchemaList is required"},
        )

    columns = []
    for field in field_list:
        if not isinstance(field, dict):
            continue
        field_path = str(field.get("fieldPath") or "").strip()
        field_type_obj = field.get("type") or {}
        field_type = str(field_type_obj.get("type") or "string") if isinstance(field_type_obj, dict) else "string"
        if field_path:
            columns.append({"name": field_path, "type": field_type})

    new_schema = {"columns": columns}

    try:
        await dataset_registry.update_schema(dataset_id=dataset_id, schema_json=new_schema)
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="PutSchemaFailed",
            log_message="Failed to update schema",
        )

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/getSchemaBatch",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.get_schema_batch")
async def get_schema_batch_v2(
    request: Request,
    preview: bool = Query(..., alias="preview"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """POST /v2/datasets/getSchemaBatch — Get dataset schemas batch (preview)."""
    if preview is not True:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="ApiFeaturePreviewUsageOnly",
            parameters={},
        )

    try:
        body = await request.json()
    except _DATASETS_JSON_EXCEPTIONS:
        return _datasets_invalid_json_response()

    dataset_rids = body.get("datasetRids")
    if not isinstance(dataset_rids, list) or not dataset_rids:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "datasetRids is required"},
        )

    output: list[dict[str, Any]] = []
    for raw in dataset_rids:
        rid = str(raw or "").strip()
        dataset_id = _dataset_id_from_rid(rid)
        if not dataset_id:
            return foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidDatasetRid",
                parameters={"datasetRid": rid},
            )
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            return foundry_error(
                404,
                error_code="NOT_FOUND",
                error_name="DatasetNotFound",
                parameters={"datasetRid": rid},
            )
        schema_json = dataset.schema_json if isinstance(dataset.schema_json, dict) else {}
        if not schema_json or not schema_json.get("columns"):
            return foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="SchemaNotFound",
                parameters={"datasetRid": rid},
            )
        output.append({"datasetRid": _dataset_rid(dataset_id), "schema": _schema_response(schema_json)})

    return JSONResponse(content=output)


# ---------------------------------------------------------------------------
# Branch management
# ---------------------------------------------------------------------------

async def _get_lakefs_client(pipeline_registry: PipelineRegistry, request: Request) -> LakeFSClient:
    actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
    return await pipeline_registry.get_lakefs_client(user_id=actor_user_id)


async def _get_lakefs_storage(pipeline_registry: PipelineRegistry, request: Request) -> Any:
    actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
    return await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)


@router.get(
    "/{datasetRid}/branches",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.list_branches")
async def list_branches_v2(
    datasetRid: str,
    request: Request,
    pageSize: int = Query(default=100, ge=1, le=1000, alias="pageSize"),
    pageToken: str = Query(default="", alias="pageToken"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/branches — List branches."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    repo = _resolve_lakefs_raw_repository()
    rid = _dataset_rid(dataset_id)
    scope_key = f"datasets:branches:{dataset_id}"
    try:
        offset = decode_offset_page_token(
            pageToken,
            ttl_seconds=60 * 60 * 24,
            expected_scope=scope_key,
        )
    except ValueError as exc:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        branches = await lakefs_client.list_branches(repository=repo)
    except LakeFSError as exc:
        logger.warning("Failed to list lakeFS branches: %s", exc)
        branches = [{"name": dataset.branch or "master"}]
    except _DATASETS_HANDLED_EXCEPTIONS:
        branches = [{"name": dataset.branch or "master"}]

    sliced = branches[int(offset) : int(offset) + int(pageSize) + 1]
    has_next = len(sliced) > int(pageSize)
    if has_next:
        sliced = sliced[: int(pageSize)]

    data: list[dict[str, Any]] = []
    for br in sliced:
        br_name = br.get("name") if isinstance(br, dict) else str(br)
        commit_id = br.get("commit_id") if isinstance(br, dict) else None
        data.append(_branch_response(branch_name=br_name, dataset_rid=rid, commit_id=commit_id))

    next_token = None
    if has_next:
        next_token = encode_offset_page_token(int(offset) + int(pageSize), scope=scope_key)

    return JSONResponse(content={"data": data, "nextPageToken": next_token})


@router.post(
    "/{datasetRid}/branches",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.create_branch")
async def create_branch_v2(
    datasetRid: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-write"],
    ),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/branches — Create branch."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    try:
        body = await request.json()
    except _DATASETS_JSON_EXCEPTIONS:
        return _datasets_invalid_json_response()

    branch_name = str(body.get("name") or "").strip()
    if not branch_name:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "name is required"},
        )

    try:
        branch_name = validate_branch_name(branch_name)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": branch_name},
        )

    source_branch = str(body.get("sourceBranchName") or "master").strip() or "master"
    try:
        source_branch = validate_branch_name(source_branch)
    except SecurityViolationError as exc:
        logger.info("Invalid source branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": source_branch},
        )
    repo = _resolve_lakefs_raw_repository()

    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        await lakefs_client.create_branch(repository=repo, name=branch_name, source=source_branch)
    except LakeFSConflictError:
        return foundry_error(
            409,
            error_code="CONFLICT",
            error_name="BranchAlreadyExists",
            parameters={"branchName": branch_name},
        )
    except LakeFSNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="SourceBranchNotFound",
            parameters={"sourceBranchName": source_branch},
        )
    except LakeFSError as exc:
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="BranchCreationFailed",
            parameters={"message": str(exc)},
        )

    rid = _dataset_rid(dataset_id)
    return JSONResponse(content=_branch_response(branch_name=branch_name, dataset_rid=rid))


@router.get(
    "/{datasetRid}/branches/{branchName}",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.get_branch")
async def get_branch_v2(
    datasetRid: str,
    branchName: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/branches/{branchName} — Get branch."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    normalized_branch = str(branchName or "").strip()
    try:
        normalized_branch = validate_branch_name(normalized_branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": normalized_branch},
        )

    repo = _resolve_lakefs_raw_repository()
    commit_id: str | None = None
    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        commit_id = await lakefs_client.get_branch_head_commit_id(repository=repo, branch=normalized_branch)
    except LakeFSNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="BranchNotFound",
            parameters={"branchName": normalized_branch, "datasetRid": datasetRid},
        )
    except LakeFSError as exc:
        logger.warning("Failed to get branch head: %s", exc)

    rid = _dataset_rid(dataset_id)
    return JSONResponse(content=_branch_response(branch_name=normalized_branch, dataset_rid=rid, commit_id=commit_id))


@router.delete(
    "/{datasetRid}/branches/{branchName}",
    status_code=status.HTTP_204_NO_CONTENT,
)
@trace_endpoint("foundry_datasets_v2.delete_branch")
async def delete_branch_v2(
    datasetRid: str,
    branchName: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-write"],
    ),
) -> Response:
    """DELETE /v2/datasets/{datasetRid}/branches/{branchName} — Delete branch."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    normalized_branch = str(branchName or "").strip()
    try:
        normalized_branch = validate_branch_name(normalized_branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": normalized_branch},
        )

    if normalized_branch == "master":
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="CannotDeleteMasterBranch",
            parameters={"branchName": "master"},
        )

    repo = _resolve_lakefs_raw_repository()
    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        await lakefs_client.delete_branch(repository=repo, name=normalized_branch)
    except LakeFSNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="BranchNotFound",
            parameters={"branchName": normalized_branch},
        )
    except LakeFSError as exc:
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="BranchDeletionFailed",
            parameters={"message": str(exc)},
        )

    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

@router.post(
    "/{datasetRid}/transactions",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.create_transaction")
async def create_transaction_v2(
    datasetRid: str,
    request: Request,
    branchName: str = Query(default="master", alias="branchName"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-write"],
    ),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/transactions — Create a transaction."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    try:
        body = await request.json()
    except _DATASETS_JSON_EXCEPTIONS:
        return _datasets_invalid_json_response()

    transaction_type = str(body.get("transactionType") or "").strip().upper()
    if transaction_type not in {"APPEND", "UPDATE"}:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "transactionType must be APPEND or UPDATE"},
        )

    branch_name = str(branchName or "master").strip() or "master"
    try:
        branch_name = validate_branch_name(branch_name)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": branch_name},
        )

    idempotency_key = str(uuid4())

    try:
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset_id,
            db_name=dataset.db_name,
            branch=branch_name,
            idempotency_key=idempotency_key,
            request_fingerprint=None,
            source_metadata={"transactionType": transaction_type},
        )
        txn = await dataset_registry.create_ingest_transaction(
            ingest_request_id=ingest_request.ingest_request_id,
            status="OPEN",
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="CreateTransactionError",
            log_message="Failed to create transaction",
        )

    created_time = txn.created_at.isoformat() if getattr(txn, "created_at", None) else None
    return JSONResponse(
        content={
            "rid": _transaction_rid(txn.transaction_id),
            "transactionType": transaction_type,
            "status": _map_txn_status(txn.status),
            **({"createdTime": created_time} if created_time else {}),
        }
    )


@router.get(
    "/{datasetRid}/transactions",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.list_transactions")
async def list_transactions_v2(
    datasetRid: str,
    preview: bool = Query(..., alias="preview"),
    branchName: str = Query(default="master", alias="branchName"),
    pageSize: int = Query(default=100, ge=1, le=1000, alias="pageSize"),
    pageToken: str = Query(default="", alias="pageToken"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/transactions — List transactions."""
    if preview is not True:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="ApiFeaturePreviewUsageOnly",
            parameters={},
        )

    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    normalized_branch = str(branchName or "").strip() or "master"
    try:
        normalized_branch = validate_branch_name(normalized_branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": normalized_branch},
        )

    scope_key = f"datasets:transactions:{dataset_id}:{normalized_branch}"
    try:
        offset = decode_offset_page_token(
            pageToken,
            ttl_seconds=60 * 60 * 24,
            expected_scope=scope_key,
        )
    except ValueError as exc:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    try:
        records = await dataset_registry.list_ingest_transactions_for_dataset(
            dataset_id=dataset_id,
            branch=normalized_branch,
            limit=int(pageSize) + 1,
            offset=int(offset),
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="ListTransactionsError",
            log_message="Failed to list transactions",
        )

    has_next = len(records) > int(pageSize)
    page = records[: int(pageSize)]
    next_token = None
    if has_next:
        next_token = encode_offset_page_token(int(offset) + int(pageSize), scope=scope_key)

    data: list[dict[str, Any]] = []
    for row in page:
        transaction_type: str = "APPEND"
        try:
            ingest_request = await dataset_registry.get_ingest_request(ingest_request_id=row.ingest_request_id)
            source_metadata = getattr(ingest_request, "source_metadata", None) if ingest_request else None
            if isinstance(source_metadata, dict) and source_metadata.get("transactionType"):
                transaction_type = str(source_metadata.get("transactionType")).strip().upper() or transaction_type
        except _DATASETS_HANDLED_EXCEPTIONS:
            transaction_type = "APPEND"

        created_time = row.created_at.isoformat() if getattr(row, "created_at", None) else None
        closed = getattr(row, "committed_at", None) or getattr(row, "aborted_at", None)
        closed_time = closed.isoformat() if hasattr(closed, "isoformat") else None
        data.append(
            {
                "rid": _transaction_rid(row.transaction_id),
                "transactionType": transaction_type,
                "status": _map_txn_status(row.status),
                "createdTime": created_time,
                "closedTime": closed_time,
            }
        )

    return JSONResponse(content={"data": data, "nextPageToken": next_token})


@router.get(
    "/{datasetRid}/transactions/{transactionRid}",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.get_transaction")
async def get_transaction_v2(
    datasetRid: str,
    transactionRid: str,
    branchName: str = Query(default="master", alias="branchName"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/transactions/{transactionRid} — Get transaction."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    transaction_id = _transaction_id_from_rid(transactionRid)
    if not transaction_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidTransactionRid",
            parameters={"transactionRid": transactionRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    txn = await dataset_registry.get_ingest_transaction_by_id(transaction_id=transaction_id)
    if not txn:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="TransactionNotFound",
            parameters={"transactionRid": transactionRid},
        )

    # Ensure the transaction belongs to this dataset.
    ingest_request = await dataset_registry.get_ingest_request(ingest_request_id=txn.ingest_request_id)
    if not ingest_request or str(getattr(ingest_request, "dataset_id", "")) != str(dataset_id):
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="TransactionNotFound",
            parameters={"transactionRid": transactionRid},
        )

    normalized_branch = str(branchName or "master").strip() or "master"
    try:
        normalized_branch = validate_branch_name(normalized_branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": normalized_branch},
        )

    if str(getattr(ingest_request, "branch", "") or "") != str(normalized_branch):
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="TransactionNotFound",
            parameters={"transactionRid": transactionRid},
        )

    transaction_type = "APPEND"
    source_metadata = getattr(ingest_request, "source_metadata", None)
    if isinstance(source_metadata, dict) and source_metadata.get("transactionType"):
        transaction_type = str(source_metadata.get("transactionType")).strip().upper() or transaction_type

    created_time = txn.created_at.isoformat() if getattr(txn, "created_at", None) else None
    closed = getattr(txn, "committed_at", None) or getattr(txn, "aborted_at", None)
    closed_time = closed.isoformat() if hasattr(closed, "isoformat") else None
    return JSONResponse(
        content={
            "rid": _transaction_rid(txn.transaction_id),
            "transactionType": transaction_type,
            "status": _map_txn_status(txn.status),
            "createdTime": created_time,
            "closedTime": closed_time,
        }
    )


@router.post(
    "/{datasetRid}/transactions/{transactionRid}/commit",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.commit_transaction")
async def commit_transaction_v2(
    datasetRid: str,
    transactionRid: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-write"],
    ),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/transactions/{transactionRid}/commit."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    transaction_id = _transaction_id_from_rid(transactionRid)
    if not transaction_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidTransactionRid",
            parameters={"transactionRid": transactionRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    # Transaction RID resolves to transaction_id, not ingest_request_id.
    txn = await dataset_registry.get_ingest_transaction_by_id(transaction_id=transaction_id)
    if not txn:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="TransactionNotFound",
            parameters={"transactionRid": transactionRid},
        )

    if txn.status != "OPEN":
        return foundry_error(
            409,
            error_code="CONFLICT",
            error_name="TransactionNotOpen",
            parameters={"transactionRid": transactionRid, "status": txn.status},
        )

    ingest_request = await dataset_registry.get_ingest_request(ingest_request_id=txn.ingest_request_id)
    if not ingest_request:
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="CommitTransactionError",
            parameters={"transactionRid": transactionRid, "datasetRid": datasetRid, "message": "Ingest request not found"},
        )

    branch = str(getattr(ingest_request, "branch", "") or "").strip() or "master"
    # Commit to lakeFS
    repo = _resolve_lakefs_raw_repository()
    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        commit_id = await lakefs_client.commit(
            repository=repo,
            branch=branch,
            message=f"Transaction commit for dataset {dataset.name}",
            metadata={"dataset_id": dataset_id, "transaction_id": transaction_id},
        )
    except LakeFSError as exc:
        logger.error("lakeFS commit failed during transaction commit: %s", exc)
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="CommitTransactionError",
            parameters={
                "transactionRid": transactionRid,
                "datasetRid": datasetRid,
                "message": "lakeFS commit failed; transaction remains OPEN",
            },
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="CommitTransactionError",
            parameters={"transactionRid": transactionRid, "datasetRid": datasetRid},
            log_message="Unexpected transaction commit failure",
        )

    lakefs_storage_service = await _get_lakefs_storage(pipeline_registry, request)
    artifact_key, sample_json, schema_json, row_count = await _extract_commit_preview(
        dataset=dataset,
        branch=branch,
        repository=repo,
        lakefs_client=lakefs_client,
        lakefs_storage_service=lakefs_storage_service,
    )
    committed_artifact_key = _artifact_s3_uri(
        repository=repo,
        branch=branch,
        object_key=artifact_key,
    )
    if not committed_artifact_key:
        committed_artifact_key = _artifact_s3_uri(
            repository=repo,
            branch=branch,
            object_key=_normalize_dataset_object_key(dataset, "source.csv"),
        )
    try:
        await dataset_registry.mark_ingest_committed(
            ingest_request_id=ingest_request.ingest_request_id,
            lakefs_commit_id=commit_id,
            artifact_key=committed_artifact_key,
        )
        await dataset_registry.mark_ingest_transaction_committed(
            ingest_request_id=ingest_request.ingest_request_id,
            lakefs_commit_id=commit_id,
            artifact_key=committed_artifact_key,
        )
    except _DATASETS_HANDLED_EXCEPTIONS as raw_commit_exc:
        logger.warning("Failed to mark ingest RAW_COMMITTED before publish: %s", raw_commit_exc)

    try:
        apply_schema = bool(schema_json.get("columns")) if isinstance(schema_json, dict) else False
        await dataset_registry.publish_ingest_request(
            ingest_request_id=ingest_request.ingest_request_id,
            dataset_id=dataset_id,
            lakefs_commit_id=commit_id,
            artifact_key=committed_artifact_key,
            row_count=row_count,
            sample_json=sample_json,
            schema_json=schema_json,
            apply_schema=apply_schema,
        )
    except ValueError as exc:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="TransactionCommitFailed",
            parameters={"transactionRid": transactionRid, "datasetRid": datasetRid, "message": str(exc)},
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to publish ingest request during transaction commit: %s", exc)
        try:
            await dataset_registry.reconcile_ingest_state(
                stale_after_seconds=60,
                limit=50,
                use_lock=False,
            )
            recovered = await dataset_registry.get_latest_version(dataset_id=dataset_id)
            recovered_commit_id = str(getattr(recovered, "lakefs_commit_id", "") or "").strip()
            if recovered is None or recovered_commit_id != str(commit_id):
                return foundry_error(
                    500,
                    error_code="INTERNAL",
                    error_name="TransactionCommitFailed",
                    parameters={"transactionRid": transactionRid, "datasetRid": datasetRid, "message": str(exc)},
                )
            logger.warning(
                "Recovered dataset version through reconcile after publish failure "
                "(dataset_id=%s, commit_id=%s)",
                dataset_id,
                commit_id,
            )
        except _DATASETS_HANDLED_EXCEPTIONS as reconcile_exc:
            return foundry_error(
                500,
                error_code="INTERNAL",
                error_name="TransactionCommitFailed",
                parameters={
                    "transactionRid": transactionRid,
                    "datasetRid": datasetRid,
                    "message": f"{exc} (reconcile failed: {reconcile_exc})",
                },
            )

    committed_txn = await dataset_registry.get_ingest_transaction_by_id(transaction_id=transaction_id)
    if not committed_txn:
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="TransactionCommitFailed",
            parameters={"transactionRid": transactionRid},
        )

    return JSONResponse(
        content={
            "rid": _transaction_rid(transaction_id),
            "status": _map_txn_status(committed_txn.status),
        }
    )


@router.post(
    "/{datasetRid}/transactions/{transactionRid}/abort",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.abort_transaction")
async def abort_transaction_v2(
    datasetRid: str,
    transactionRid: str,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(
        ["api:datasets-write"],
    ),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/transactions/{transactionRid}/abort."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    transaction_id = _transaction_id_from_rid(transactionRid)
    if not transaction_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidTransactionRid",
            parameters={"transactionRid": transactionRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    txn = await dataset_registry.get_ingest_transaction_by_id(transaction_id=transaction_id)
    if not txn:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="TransactionNotFound",
            parameters={"transactionRid": transactionRid},
        )

    if txn.status != "OPEN":
        return foundry_error(
            409,
            error_code="CONFLICT",
            error_name="TransactionNotOpen",
            parameters={"transactionRid": transactionRid, "status": txn.status},
        )

    aborted_txn = await dataset_registry.mark_ingest_transaction_aborted(
        ingest_request_id=txn.ingest_request_id,
        error="Transaction aborted by user",
    )
    if not aborted_txn:
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="TransactionAbortFailed",
            parameters={"transactionRid": transactionRid},
        )

    return JSONResponse(
        content={
            "rid": _transaction_rid(transaction_id),
            "status": _map_txn_status(aborted_txn.status),
        }
    )


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

@router.get(
    "/{datasetRid}/files",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.list_files")
async def list_files_v2(
    datasetRid: str,
    request: Request,
    branchName: str = Query(default="master", alias="branchName"),
    startTransactionRid: str = Query(default="", alias="startTransactionRid"),
    endTransactionRid: str = Query(default="", alias="endTransactionRid"),
    prefix: str = Query(default="", alias="prefix"),
    pageSize: int = Query(default=100, ge=1, le=1000, alias="pageSize"),
    pageToken: str = Query(default="", alias="pageToken"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/files — List files in dataset."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    repo = _resolve_lakefs_raw_repository()
    ref = str(branchName or "").strip() or "master"
    try:
        ref = validate_branch_name(ref)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": ref},
        )

    if startTransactionRid and not _transaction_id_from_rid(startTransactionRid):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "startTransactionRid is invalid"},
        )
    if endTransactionRid and not _transaction_id_from_rid(endTransactionRid):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "endTransactionRid is invalid"},
        )

    # Build prefix from dataset naming convention
    dataset_prefix = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/"
    file_prefix = str(prefix or "").lstrip("/")
    object_prefix = f"{dataset_prefix}{file_prefix}" if file_prefix else dataset_prefix

    scope_key = f"datasets:files:{dataset_id}:{ref}:{file_prefix}"
    try:
        offset = decode_offset_page_token(
            pageToken,
            ttl_seconds=60 * 60 * 24,
            expected_scope=scope_key,
        )
    except ValueError as exc:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    files: List[Dict[str, Any]] = []
    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        objects = await lakefs_client.list_objects(repository=repo, ref=ref, prefix=object_prefix, amount=1000)
        sliced = objects[int(offset) : int(offset) + int(pageSize) + 1]
        has_next = len(sliced) > int(pageSize)
        if has_next:
            sliced = sliced[: int(pageSize)]

        for obj in sliced:
            path = obj.get("path", "")
            # Strip the dataset prefix for display
            relative_path = path[len(dataset_prefix) :] if path.startswith(dataset_prefix) else path
            relative_path = relative_path.lstrip("/")
            files.append(
                {
                    "rid": _file_rid(dataset_id=dataset_id, file_path=relative_path),
                    "filePath": relative_path,
                    "sizeBytes": obj.get("size_bytes") or obj.get("sizeBytes") or 0,
                    "updatedTime": obj.get("mtime") or None,
                }
            )
        next_token = None
        if has_next:
            next_token = encode_offset_page_token(int(offset) + int(pageSize), scope=scope_key)
    except LakeFSError as exc:
        logger.error("lakeFS list_files failed: %s", exc)
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="ListFilesError",
            parameters={"datasetRid": datasetRid, "message": str(exc)},
        )
    except LakeFSNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="BranchNotFound",
            parameters={"branchName": ref, "datasetRid": datasetRid},
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="ListFilesError",
            parameters={"datasetRid": datasetRid},
            log_message="Unexpected list_files failure",
        )

    return JSONResponse(content={"data": files, "nextPageToken": next_token})


@router.get(
    "/{datasetRid}/files/{filePath:path}/content",
    status_code=status.HTTP_200_OK,
    response_model=None,
)
@trace_endpoint("foundry_datasets_v2.get_file_content")
async def get_file_content_v2(
    datasetRid: str,
    filePath: str,
    request: Request,
    branchName: str = Query(default="master", alias="branchName"),
    startTransactionRid: str = Query(default="", alias="startTransactionRid"),
    endTransactionRid: str = Query(default="", alias="endTransactionRid"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> Response:
    """GET /v2/datasets/{datasetRid}/files/{filePath}/content — Download file."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    repo = _resolve_lakefs_raw_repository()
    branch = str(branchName or "").strip() or (dataset.branch or "master")
    if startTransactionRid and not _transaction_id_from_rid(startTransactionRid):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "startTransactionRid is invalid"},
        )
    if endTransactionRid and not _transaction_id_from_rid(endTransactionRid):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "endTransactionRid is invalid"},
        )

    try:
        branch = validate_branch_name(branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": branch},
        )

    normalized_path = str(filePath or "").strip().lstrip("/")
    object_key = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/{normalized_path}"

    try:
        storage_service = await _get_lakefs_storage(pipeline_registry, request)
        content = await storage_service.load_bytes(repo, f"{branch}/{object_key}")
    except FileNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="FileNotFoundOnBranch",
            parameters={"branchName": branch, "filePath": normalized_path, "datasetRid": datasetRid},
        )
    except LakeFSNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="BranchNotFound",
            parameters={"branchName": branch, "datasetRid": datasetRid},
        )
    except LakeFSError as exc:
        logger.error("lakeFS get_file_content failed: %s", exc)
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="GetFileContentError",
            parameters={"datasetRid": datasetRid, "message": str(exc)},
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="GetFileContentError",
            parameters={"datasetRid": datasetRid},
            log_message="Failed to load file content",
        )

    media_type = mimetypes.guess_type(filePath)[0] or "application/octet-stream"
    return Response(content=content, media_type=media_type)


@router.post(
    "/{datasetRid}/files/{filePath:path}/upload",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.upload_file")
async def upload_file_v2(
    datasetRid: str,
    filePath: str,
    request: Request,
    branchName: str = Query(default="master", alias="branchName"),
    transactionRid: str = Query(default="", alias="transactionRid"),
    byteOffset: int | None = Query(default=None, alias="byteOffset"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-write"],
    ),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/files/{filePath}/upload — Upload file."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    file_path = str(filePath or "").strip().lstrip("/")
    if not file_path:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "filePath is required"},
        )

    repo = _resolve_lakefs_raw_repository()
    txn_id: str | None = None
    transaction_record: Any | None = None
    ingest_request: Any | None = None

    if transactionRid:
        txn_id = _transaction_id_from_rid(transactionRid)
        if not txn_id:
            return foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidTransactionRid",
                parameters={"transactionRid": transactionRid},
            )
        transaction_record = await dataset_registry.get_ingest_transaction_by_id(transaction_id=txn_id)
        if not transaction_record:
            return foundry_error(
                404,
                error_code="NOT_FOUND",
                error_name="TransactionNotFound",
                parameters={"transactionRid": transactionRid},
            )
        if transaction_record.status != "OPEN":
            return foundry_error(
                409,
                error_code="CONFLICT",
                error_name="TransactionNotOpen",
                parameters={"transactionRid": transactionRid, "status": transaction_record.status},
            )
        ingest_request = await dataset_registry.get_ingest_request(ingest_request_id=transaction_record.ingest_request_id)
        if not ingest_request or str(getattr(ingest_request, "dataset_id", "")) != str(dataset_id):
            return foundry_error(
                404,
                error_code="NOT_FOUND",
                error_name="TransactionNotFound",
                parameters={"transactionRid": transactionRid},
            )
        branch = str(getattr(ingest_request, "branch", "") or "").strip() or "master"
    else:
        branch = str(branchName or "").strip() or "master"
        try:
            branch = validate_branch_name(branch)
        except SecurityViolationError as exc:
            logger.info("Invalid branch name rejected: %s", exc)
            return foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidBranchName",
                parameters={"branchName": branch},
            )
        try:
            ingest_request, _ = await dataset_registry.create_ingest_request(
                dataset_id=dataset_id,
                db_name=dataset.db_name,
                branch=branch,
                idempotency_key=str(uuid4()),
                request_fingerprint=None,
            )
            transaction_record = await dataset_registry.create_ingest_transaction(
                ingest_request_id=ingest_request.ingest_request_id,
                status="OPEN",
            )
            txn_id = transaction_record.transaction_id
        except _DATASETS_HANDLED_EXCEPTIONS as exc:
            return _datasets_internal_error_response(
                exc=exc,
                error_name="UploadFileError",
                log_message="Failed to create upload transaction",
            )
    object_key = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/{file_path}"

    try:
        body_bytes = await request.body()
        content_type = request.headers.get("Content-Type") or "application/octet-stream"
        storage_service = await _get_lakefs_storage(pipeline_registry, request)
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        await _ensure_lakefs_branch_exists(
            lakefs_client=lakefs_client,
            repository=repo,
            branch=branch,
        )
        resolved_offset = int(byteOffset) if byteOffset is not None else 0
        if resolved_offset < 0:
            return foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "byteOffset must be >= 0"},
            )

        payload_bytes = body_bytes
        if resolved_offset:
            try:
                existing = await storage_service.load_bytes(repo, f"{branch}/{object_key}")
            except (FileNotFoundError, LakeFSNotFoundError):
                return foundry_error(
                    400,
                    error_code="INVALID_ARGUMENT",
                    error_name="WriteOffsetOutOfBounds",
                    parameters={
                        "datasetRid": datasetRid,
                        "filePath": file_path,
                        "branchName": branch,
                        "byteOffset": resolved_offset,
                    },
                )
            if len(existing) != resolved_offset:
                return foundry_error(
                    400,
                    error_code="INVALID_ARGUMENT",
                    error_name="WriteOffsetOutOfBounds",
                    parameters={
                        "datasetRid": datasetRid,
                        "filePath": file_path,
                        "branchName": branch,
                        "byteOffset": resolved_offset,
                        "currentSizeBytes": len(existing),
                    },
                )
            payload_bytes = existing + body_bytes

        await storage_service.save_bytes(repo, f"{branch}/{object_key}", payload_bytes, content_type=content_type)
        # Keep a canonical CSV object so commit/readTable can materialize even when
        # callers upload arbitrary CSV file paths instead of source.csv.
        if file_path.lower().endswith(".csv") and file_path.lower() != "source.csv":
            canonical_key = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/source.csv"
            await storage_service.save_bytes(repo, f"{branch}/{canonical_key}", payload_bytes, content_type=content_type)
    except LakeFSNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="BranchNotFound",
            parameters={"branchName": branch, "datasetRid": datasetRid},
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="UploadFileError",
            log_message="Failed to upload file",
        )

    return JSONResponse(
        content={
            "rid": _file_rid(dataset_id=dataset_id, file_path=file_path),
            "transactionRid": _transaction_rid(str(txn_id or "")),
        }
    )


@router.get(
    "/{datasetRid}/files/{filePath:path}",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.get_file")
async def get_file_v2(
    datasetRid: str,
    filePath: str,
    request: Request,
    branchName: str = Query(default="master", alias="branchName"),
    startTransactionRid: str = Query(default="", alias="startTransactionRid"),
    endTransactionRid: str = Query(default="", alias="endTransactionRid"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/files/{filePath} — Get file metadata."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    normalized_path = str(filePath or "").strip().lstrip("/")
    if not normalized_path:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "filePath is required"},
        )

    branch = str(branchName or "").strip() or (dataset.branch or "master")
    try:
        branch = validate_branch_name(branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": branch},
        )

    if startTransactionRid and not _transaction_id_from_rid(startTransactionRid):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "startTransactionRid is invalid"},
        )
    if endTransactionRid and not _transaction_id_from_rid(endTransactionRid):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "endTransactionRid is invalid"},
        )

    repo = _resolve_lakefs_raw_repository()
    object_key = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/{normalized_path}"
    try:
        storage_service = await _get_lakefs_storage(pipeline_registry, request)
        meta = await storage_service.get_object_metadata(repo, f"{branch}/{object_key}")
    except FileNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="FileNotFoundOnBranch",
            parameters={"branchName": branch, "filePath": normalized_path, "datasetRid": datasetRid},
        )
    except LakeFSNotFoundError:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="BranchNotFound",
            parameters={"branchName": branch, "datasetRid": datasetRid},
        )
    except LakeFSError as exc:
        logger.error("lakeFS get_file failed: %s", exc)
        return foundry_error(
            500,
            error_code="INTERNAL",
            error_name="GetFileError",
            parameters={"datasetRid": datasetRid, "message": str(exc)},
        )
    except _DATASETS_HANDLED_EXCEPTIONS as exc:
        return _datasets_internal_error_response(
            exc=exc,
            error_name="GetFileError",
            parameters={"datasetRid": datasetRid},
            log_message="Failed to load file metadata",
        )

    updated = meta.get("last_modified")
    updated_time = updated.isoformat() if hasattr(updated, "isoformat") else (str(updated) if updated else None)
    size = meta.get("size")
    try:
        size_bytes = int(size) if size is not None else 0
    except (TypeError, ValueError):
        size_bytes = 0

    return JSONResponse(
        content={
            "rid": _file_rid(dataset_id=dataset_id, file_path=normalized_path),
            "filePath": normalized_path,
            "sizeBytes": size_bytes,
            "updatedTime": updated_time,
        }
    )


# ---------------------------------------------------------------------------
# Read table
# ---------------------------------------------------------------------------

@router.get(
    "/{datasetRid}/readTable",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.read_table")
async def read_table_v2(
    datasetRid: str,
    request: Request,
    branchName: str = Query(default="master", alias="branchName"),
    startTransactionRid: str = Query(default="", alias="startTransactionRid"),
    endTransactionRid: str = Query(default="", alias="endTransactionRid"),
    format: str = Query(..., alias="format"),
    columns: list[str] | None = Query(default=None, alias="columns"),
    rowLimit: int = Query(default=100, alias="rowLimit"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(
        ["api:datasets-read"],
    ),
) -> Response:
    """GET /v2/datasets/{datasetRid}/readTable — Read table rows."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidDatasetRid",
            parameters={"datasetRid": datasetRid},
        )

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return foundry_error(
            404,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": datasetRid},
        )

    normalized_branch = str(branchName or "").strip() or "master"
    try:
        normalized_branch = validate_branch_name(normalized_branch)
    except SecurityViolationError as exc:
        logger.info("Invalid branch name rejected: %s", exc)
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidBranchName",
            parameters={"branchName": normalized_branch},
        )

    if startTransactionRid and not _transaction_id_from_rid(startTransactionRid):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "startTransactionRid is invalid"},
        )
    if endTransactionRid and not _transaction_id_from_rid(endTransactionRid):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "endTransactionRid is invalid"},
        )

    try:
        requested_limit = int(rowLimit)
    except (TypeError, ValueError):
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "rowLimit must be an integer"},
        )
    if requested_limit < 1:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "rowLimit must be >= 1"},
        )
    if requested_limit > 10000:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="ReadTableRowLimitExceeded",
            parameters={"rowLimit": requested_limit, "maxRowLimit": 10000},
        )

    fmt = str(format or "").strip().upper()
    if fmt not in {"CSV", "ARROW"}:
        return foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "format must be ARROW or CSV"},
        )

    if fmt == "ARROW":
        try:
            import pyarrow as pa  # type: ignore
            import pyarrow.ipc as pa_ipc  # type: ignore
        except ModuleNotFoundError:
            return foundry_error(
                500,
                error_code="INTERNAL",
                error_name="ReadTableError",
                parameters={"datasetRid": datasetRid, "message": "pyarrow is not available"},
            )

    requested_columns = [str(col).strip() for col in (columns or []) if str(col).strip()]

    headers: list[str] = []
    rows: list[list[Any]] = []
    total_count: int = 0

    version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
    if not version:
        try:
            await dataset_registry.reconcile_ingest_state(
                stale_after_seconds=60,
                limit=50,
                use_lock=False,
            )
            version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
        except _DATASETS_HANDLED_EXCEPTIONS as reconcile_exc:
            logger.warning("Dataset ingest reconcile before readTable failed: %s", reconcile_exc)

    if version and getattr(version, "sample_json", None):
        sample = version.sample_json or {}
        cached_columns = sample.get("columns") or []
        cached_rows = sample.get("rows") or []
        if isinstance(cached_rows, list) and cached_rows:
            headers = [
                str(col.get("name") or "").strip() if isinstance(col, dict) else str(col).strip()
                for col in cached_columns
            ]
            headers = [h for h in headers if h]
            if not headers and isinstance(cached_rows[0], dict):
                headers = [str(key).strip() for key in cached_rows[0].keys() if str(key).strip()]
            total_count = int(getattr(version, "row_count", None) or len(cached_rows))
            for raw in cached_rows[:requested_limit]:
                if isinstance(raw, list):
                    rows.append(raw)
                elif isinstance(raw, dict) and headers:
                    rows.append([raw.get(name) for name in headers])

    if not rows:
        repo = _resolve_lakefs_raw_repository()
        try:
            storage_service = await _get_lakefs_storage(pipeline_registry, request)
            fallback_keys: list[str] = []
            artifact_key = getattr(version, "artifact_key", None) if version else None
            if isinstance(artifact_key, str) and artifact_key.strip():
                fallback_keys.append(_normalize_dataset_object_key(dataset, artifact_key))
            fallback_keys.append(_normalize_dataset_object_key(dataset, "source.csv"))

            for object_key in dict.fromkeys(fallback_keys):
                try:
                    csv_bytes = await storage_service.load_bytes(repo, f"{normalized_branch}/{object_key}")
                except (FileNotFoundError, LakeFSNotFoundError):
                    continue

                reader = csv.reader(io.StringIO(csv_bytes.decode("utf-8-sig", errors="replace")))
                all_rows = list(reader)
                if not all_rows:
                    continue
                headers = _normalized_csv_headers([str(cell) for cell in all_rows[0]])
                raw_data_rows = all_rows[1:]
                total_count = len(raw_data_rows)
                rows = raw_data_rows[:requested_limit]
                break
        except LakeFSError as exc:
            logger.error("lakeFS readTable load failed: %s", exc)
            return foundry_error(
                500,
                error_code="INTERNAL",
                error_name="ReadTableError",
                parameters={"datasetRid": datasetRid, "message": "lakeFS read failed"},
            )

    if requested_columns and headers:
        index_by_name = {name: idx for idx, name in enumerate(headers)}
        selected_indices = [index_by_name[name] for name in requested_columns if name in index_by_name]
        headers = [headers[idx] for idx in selected_indices]
        rows = [[row[idx] if idx < len(row) else None for idx in selected_indices] for row in rows]

    if fmt == "CSV":
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        if headers:
            writer.writerow(headers)
        for row in rows:
            writer.writerow([value if value is not None else "" for value in row])
        return Response(content=buffer.getvalue(), media_type="text/csv")

    # ARROW
    columns_data: dict[str, list[Any]] = {}
    for col_idx, name in enumerate(headers):
        columns_data[name] = [row[col_idx] if col_idx < len(row) else None for row in rows]
    table = pa.table({name: pa.array(values, type=pa.string()) for name, values in columns_data.items()})
    sink = io.BytesIO()
    with pa_ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return Response(content=sink.getvalue(), media_type="application/vnd.apache.arrow.file")
