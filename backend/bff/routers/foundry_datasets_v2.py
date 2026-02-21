"""Foundry Datasets API v2 — Palantir-compatible dataset lifecycle.

Implements the Foundry v2 Datasets REST surface on top of existing
DatasetRegistry (Postgres), LakeFSClient (versioned storage), and
LakeFSStorageService (S3-compatible object storage).

Endpoints
---------
Dataset CRUD:
  POST   /v2/datasets                                      Create dataset
  GET    /v2/datasets                                      List datasets
  GET    /v2/datasets/{datasetRid}                         Get dataset
  GET    /v2/datasets/{datasetRid}/schema                  Get schema
  PUT    /v2/datasets/{datasetRid}/schema                  Update schema

Branch management:
  GET    /v2/datasets/{datasetRid}/branches                List branches
  POST   /v2/datasets/{datasetRid}/branches                Create branch
  GET    /v2/datasets/{datasetRid}/branches/{branchName}   Get branch
  DELETE /v2/datasets/{datasetRid}/branches/{branchName}   Delete branch

Transactions:
  POST   /v2/datasets/{datasetRid}/transactions                          Create transaction
  POST   /v2/datasets/{datasetRid}/transactions/{txnRid}/commit          Commit
  POST   /v2/datasets/{datasetRid}/transactions/{txnRid}/abort           Abort

Files & table read:
  GET    /v2/datasets/{datasetRid}/files                   List files
  GET    /v2/datasets/{datasetRid}/files/{filePath:path}/content  Get file content
  POST   /v2/datasets/{datasetRid}/files:upload            Upload file
  POST   /v2/datasets/{datasetRid}/readTable               Read table rows
"""

import csv
import io
import logging
import mimetypes
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse

from bff.routers.data_connector_deps import get_dataset_registry, get_pipeline_registry
from bff.routers.pipeline_datasets_ops_lakefs import _resolve_lakefs_raw_repository
from shared.observability.tracing import trace_endpoint
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

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/datasets", tags=["Foundry Datasets v2"])

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
    for prefix in _DATASET_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix):].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _dataset_rid(dataset_id: str) -> str:
    return f"ri.spice.main.dataset.{dataset_id}"


def _folder_rid(db_name: str) -> str:
    return f"ri.spice.main.folder.{db_name}"


def _db_name_from_folder_rid(folder_rid: str) -> str | None:
    text = str(folder_rid or "").strip()
    if not text:
        return None
    for prefix in _FOLDER_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix):].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _transaction_rid(transaction_id: str) -> str:
    return f"ri.spice.main.transaction.{transaction_id}"


def _transaction_id_from_rid(txn_rid: str) -> str | None:
    text = str(txn_rid or "").strip()
    if not text:
        return None
    for prefix in _TRANSACTION_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix):].strip() or None
    if text.startswith("ri."):
        return None
    return text


# ---------------------------------------------------------------------------
# Error helper (matches foundry_connectivity_v2 pattern)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def _dataset_response(dataset: Any) -> Dict[str, Any]:
    created_at = dataset.created_at
    return {
        "rid": _dataset_rid(dataset.dataset_id),
        "name": dataset.name,
        "description": dataset.description,
        "parentFolderRid": _folder_rid(dataset.db_name),
        "branchName": dataset.branch,
        "sourceType": dataset.source_type,
        "createdTime": created_at.isoformat() if created_at else None,
        "createdBy": "system",
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
        branch_prefix = f"{(dataset.branch or 'main').strip('/')}/"
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
    normalized_branch = str(branch or "main").strip().strip("/") or "main"
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
    except Exception as exc:
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
        except Exception as exc:
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
) -> JSONResponse:
    """POST /v2/datasets — Create a new dataset."""
    try:
        body = await request.json()
    except Exception:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidRequestBody", parameters={"message": "Invalid JSON body"})

    name = str(body.get("name") or "").strip()
    if not name:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidArgument", parameters={"message": "name is required"})

    parent_folder_rid = str(body.get("parentFolderRid") or "").strip()
    db_name = _db_name_from_folder_rid(parent_folder_rid) if parent_folder_rid else None
    if not db_name:
        db_name = str(body.get("dbName") or body.get("db_name") or "").strip()
    if not db_name:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidArgument", parameters={"message": "parentFolderRid (or dbName) is required"})

    branch = str(body.get("branchName") or "main").strip() or "main"
    description = body.get("description")

    try:
        dataset = await dataset_registry.create_dataset(
            db_name=db_name,
            name=name,
            description=description,
            source_type="foundry_api",
            branch=branch,
        )
    except Exception as exc:
        logger.error("Failed to create dataset: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="DatasetCreationFailed", parameters={"message": str(exc)})

    return JSONResponse(content=_dataset_response(dataset))


@router.get(
    "",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.list_datasets")
async def list_datasets_v2(
    parentFolderRid: str = Query(default="", alias="parentFolderRid"),
    dbName: str = Query(default="", alias="dbName"),
    pageSize: int = Query(default=100, ge=1, le=1000, alias="pageSize"),
    pageToken: str = Query(default="", alias="pageToken"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> JSONResponse:
    """GET /v2/datasets — List datasets."""
    db_name = _db_name_from_folder_rid(parentFolderRid) if parentFolderRid else None
    if not db_name:
        db_name = dbName.strip() if dbName else None
    if not db_name:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidArgument", parameters={"message": "parentFolderRid or dbName query param required"})

    try:
        all_datasets = await dataset_registry.list_datasets(db_name=db_name)
    except Exception as exc:
        logger.error("Failed to list datasets: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="ListDatasetsFailed", parameters={"message": str(exc)})

    offset = int(pageToken) if pageToken.strip().isdigit() else 0
    page = all_datasets[offset: offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_datasets) else None

    data = []
    for ds in page:
        data.append({
            "rid": _dataset_rid(ds["dataset_id"]),
            "name": ds["name"],
            "description": ds.get("description"),
            "parentFolderRid": _folder_rid(ds["db_name"]),
            "branchName": ds.get("branch", "main"),
            "sourceType": ds.get("source_type"),
            "createdTime": ds["created_at"].isoformat() if ds.get("created_at") else None,
            "createdBy": "system",
        })

    return JSONResponse(content={"data": data, "nextPageToken": next_token})


@router.get(
    "/{datasetRid}",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.get_dataset")
async def get_dataset_v2(
    datasetRid: str,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid} — Get a dataset."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    return JSONResponse(content=_dataset_response(dataset))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

@router.get(
    "/{datasetRid}/schema",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.get_schema")
async def get_schema_v2(
    datasetRid: str,
    branchName: str = Query(default="main", alias="branchName"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/schema — Get dataset schema."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    return JSONResponse(content=_schema_response(dataset.schema_json))


@router.put(
    "/{datasetRid}/schema",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.update_schema")
async def update_schema_v2(
    datasetRid: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> JSONResponse:
    """PUT /v2/datasets/{datasetRid}/schema — Update dataset schema."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    try:
        body = await request.json()
    except Exception:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidRequestBody", parameters={"message": "Invalid JSON body"})

    field_list = body.get("fieldSchemaList")
    if not isinstance(field_list, list):
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidSchema", parameters={"message": "fieldSchemaList is required"})

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
    except Exception as exc:
        logger.error("Failed to update schema: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="SchemaUpdateFailed", parameters={"message": str(exc)})

    return JSONResponse(content=_schema_response(new_schema))


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
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/branches — List branches."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    repo = _resolve_lakefs_raw_repository()
    rid = _dataset_rid(dataset_id)

    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        branches = await lakefs_client.list_branches(repository=repo)
    except LakeFSError as exc:
        logger.warning("Failed to list lakeFS branches: %s", exc)
        branches = [{"name": dataset.branch or "main"}]
    except Exception:
        branches = [{"name": dataset.branch or "main"}]

    data = []
    for br in branches:
        br_name = br.get("name") if isinstance(br, dict) else str(br)
        commit_id = br.get("commit_id") if isinstance(br, dict) else None
        data.append(_branch_response(branch_name=br_name, dataset_rid=rid, commit_id=commit_id))

    return JSONResponse(content={"data": data})


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
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/branches — Create branch."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    try:
        body = await request.json()
    except Exception:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidRequestBody", parameters={"message": "Invalid JSON body"})

    branch_name = str(body.get("branchName") or body.get("name") or "").strip()
    if not branch_name:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidArgument", parameters={"message": "branchName is required"})

    source_branch = str(body.get("sourceBranchName") or body.get("source") or "main").strip() or "main"
    repo = _resolve_lakefs_raw_repository()

    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        await lakefs_client.create_branch(repository=repo, name=branch_name, source=source_branch)
    except LakeFSConflictError:
        return _foundry_error(409, error_code="CONFLICT", error_name="BranchAlreadyExists", parameters={"branchName": branch_name})
    except LakeFSNotFoundError:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="SourceBranchNotFound", parameters={"sourceBranchName": source_branch})
    except LakeFSError as exc:
        return _foundry_error(500, error_code="INTERNAL", error_name="BranchCreationFailed", parameters={"message": str(exc)})

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
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/branches/{branchName} — Get branch."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    repo = _resolve_lakefs_raw_repository()
    commit_id: str | None = None
    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        commit_id = await lakefs_client.get_branch_head_commit_id(repository=repo, branch=branchName)
    except LakeFSNotFoundError:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="BranchNotFound", parameters={"branchName": branchName, "datasetRid": datasetRid})
    except LakeFSError as exc:
        logger.warning("Failed to get branch head: %s", exc)

    rid = _dataset_rid(dataset_id)
    return JSONResponse(content=_branch_response(branch_name=branchName, dataset_rid=rid, commit_id=commit_id))


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
) -> Response:
    """DELETE /v2/datasets/{datasetRid}/branches/{branchName} — Delete branch."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    if branchName == "main":
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="CannotDeleteMainBranch", parameters={"branchName": "main"})

    repo = _resolve_lakefs_raw_repository()
    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        await lakefs_client.delete_branch(repository=repo, name=branchName)
    except LakeFSNotFoundError:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="BranchNotFound", parameters={"branchName": branchName})
    except LakeFSError as exc:
        return _foundry_error(500, error_code="INTERNAL", error_name="BranchDeletionFailed", parameters={"message": str(exc)})

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
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/transactions — Create a transaction."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    try:
        body = await request.json()
    except Exception:
        body = {}

    idempotency_key = str(body.get("idempotencyKey") or uuid4())

    try:
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset_id,
            db_name=dataset.db_name,
            branch=dataset.branch,
            idempotency_key=idempotency_key,
            request_fingerprint=None,
        )
        txn = await dataset_registry.create_ingest_transaction(
            ingest_request_id=ingest_request.ingest_request_id,
            status="OPEN",
        )
    except Exception as exc:
        logger.error("Failed to create transaction: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="TransactionCreationFailed", parameters={"message": str(exc)})

    resp = _transaction_response(txn)
    resp["datasetRid"] = _dataset_rid(dataset_id)
    return JSONResponse(content=resp)


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
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/transactions/{transactionRid}/commit."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    transaction_id = _transaction_id_from_rid(transactionRid)
    if not transaction_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidTransactionRid", parameters={"transactionRid": transactionRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    # Transaction RID resolves to transaction_id, not ingest_request_id.
    txn = await dataset_registry.get_ingest_transaction_by_id(transaction_id=transaction_id)
    if not txn:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="TransactionNotFound", parameters={"transactionRid": transactionRid})

    if txn.status != "OPEN":
        return _foundry_error(409, error_code="CONFLICT", error_name="TransactionAlreadyClosed", parameters={"transactionRid": transactionRid, "status": txn.status})

    # Commit to lakeFS
    repo = _resolve_lakefs_raw_repository()
    branch = dataset.branch or "main"
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
        return _foundry_error(
            503,
            error_code="SERVICE_UNAVAILABLE",
            error_name="TransactionCommitUnavailable",
            parameters={
                "transactionRid": transactionRid,
                "datasetRid": datasetRid,
                "message": "lakeFS commit failed; transaction remains OPEN",
            },
        )
    except Exception as exc:
        logger.error("Unexpected transaction commit failure: %s", exc)
        return _foundry_error(
            500,
            error_code="INTERNAL",
            error_name="TransactionCommitFailed",
            parameters={"transactionRid": transactionRid, "datasetRid": datasetRid, "message": str(exc)},
        )

    ingest_request = await dataset_registry.get_ingest_request(ingest_request_id=txn.ingest_request_id)
    if not ingest_request:
        return _foundry_error(
            500,
            error_code="INTERNAL",
            error_name="TransactionCommitFailed",
            parameters={
                "transactionRid": transactionRid,
                "datasetRid": datasetRid,
                "message": "Ingest request not found",
            },
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
    except Exception as raw_commit_exc:
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
        return _foundry_error(
            400,
            error_code="INVALID_ARGUMENT",
            error_name="TransactionCommitFailed",
            parameters={"transactionRid": transactionRid, "datasetRid": datasetRid, "message": str(exc)},
        )
    except Exception as exc:
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
                return _foundry_error(
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
        except Exception as reconcile_exc:
            return _foundry_error(
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
        return _foundry_error(500, error_code="INTERNAL", error_name="TransactionCommitFailed", parameters={"transactionRid": transactionRid})

    resp = _transaction_response(committed_txn)
    resp["datasetRid"] = _dataset_rid(dataset_id)
    return JSONResponse(content=resp)


@router.post(
    "/{datasetRid}/transactions/{transactionRid}/abort",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.abort_transaction")
async def abort_transaction_v2(
    datasetRid: str,
    transactionRid: str,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/transactions/{transactionRid}/abort."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    transaction_id = _transaction_id_from_rid(transactionRid)
    if not transaction_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidTransactionRid", parameters={"transactionRid": transactionRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    txn = await dataset_registry.get_ingest_transaction_by_id(transaction_id=transaction_id)
    if not txn:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="TransactionNotFound", parameters={"transactionRid": transactionRid})

    if txn.status != "OPEN":
        return _foundry_error(409, error_code="CONFLICT", error_name="TransactionAlreadyClosed", parameters={"transactionRid": transactionRid, "status": txn.status})

    aborted_txn = await dataset_registry.mark_ingest_transaction_aborted(
        ingest_request_id=txn.ingest_request_id,
        error="Transaction aborted by user",
    )
    if not aborted_txn:
        return _foundry_error(500, error_code="INTERNAL", error_name="TransactionAbortFailed", parameters={"transactionRid": transactionRid})

    resp = _transaction_response(aborted_txn)
    resp["datasetRid"] = _dataset_rid(dataset_id)
    return JSONResponse(content=resp)


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
    branchName: str = Query(default="main", alias="branchName"),
    pageSize: int = Query(default=100, ge=1, le=1000, alias="pageSize"),
    request: Request = None,  # type: ignore[assignment]
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> JSONResponse:
    """GET /v2/datasets/{datasetRid}/files — List files in dataset."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    repo = _resolve_lakefs_raw_repository()
    ref = branchName or dataset.branch or "main"

    # Build prefix from dataset naming convention
    prefix = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/"

    files: List[Dict[str, Any]] = []
    try:
        lakefs_client = await _get_lakefs_client(pipeline_registry, request)
        objects = await lakefs_client.list_objects(repository=repo, ref=ref, prefix=prefix, amount=pageSize)
        for obj in objects:
            path = obj.get("path", "")
            # Strip the dataset prefix for display
            relative_path = path[len(prefix):] if path.startswith(prefix) else path
            files.append({
                "path": relative_path,
                "sizeBytes": obj.get("size_bytes") or obj.get("sizeBytes") or 0,
                "transactionRid": None,
                "updatedTime": obj.get("mtime") or None,
            })
    except LakeFSError as exc:
        logger.error("lakeFS list_files failed: %s", exc)
        return _foundry_error(
            503,
            error_code="SERVICE_UNAVAILABLE",
            error_name="DatasetFilesUnavailable",
            parameters={"datasetRid": datasetRid, "message": "lakeFS list failed"},
        )
    except Exception as exc:
        logger.error("Unexpected list_files failure: %s", exc)
        return _foundry_error(
            500,
            error_code="INTERNAL",
            error_name="DatasetFilesListFailed",
            parameters={"datasetRid": datasetRid, "message": str(exc)},
        )

    return JSONResponse(content={"data": files, "nextPageToken": None})


@router.get(
    "/{datasetRid}/files/{filePath:path}/content",
    status_code=status.HTTP_200_OK,
    response_model=None,
)
@trace_endpoint("foundry_datasets_v2.get_file_content")
async def get_file_content_v2(
    datasetRid: str,
    filePath: str,
    branchName: str = Query(default="main", alias="branchName"),
    request: Request = None,  # type: ignore[assignment]
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> Response:
    """GET /v2/datasets/{datasetRid}/files/{filePath}/content — Download file."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    repo = _resolve_lakefs_raw_repository()
    branch = branchName or dataset.branch or "main"
    object_key = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/{filePath}"

    try:
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
        storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        content = await storage_service.load_bytes(repo, f"{branch}/{object_key}")
    except Exception as exc:
        logger.warning("Failed to load file content: %s", exc)
        return _foundry_error(404, error_code="NOT_FOUND", error_name="FileNotFound", parameters={"filePath": filePath})

    media_type = mimetypes.guess_type(filePath)[0] or "application/octet-stream"
    return Response(content=content, media_type=media_type)


@router.post(
    "/{datasetRid}/files:upload",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.upload_file")
async def upload_file_v2(
    datasetRid: str,
    filePath: str = Query(..., alias="filePath"),
    branchName: str = Query(default="main", alias="branchName"),
    request: Request = None,  # type: ignore[assignment]
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/files:upload — Upload file."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    file_path = str(filePath).strip()
    if not file_path:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidArgument", parameters={"message": "filePath is required"})

    repo = _resolve_lakefs_raw_repository()
    branch = branchName or dataset.branch or "main"
    object_key = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/{file_path}"

    try:
        body_bytes = await request.body()
        content_type = request.headers.get("Content-Type") or "application/octet-stream"
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
        storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        await storage_service.save_bytes(repo, f"{branch}/{object_key}", body_bytes, content_type=content_type)
        # Keep a canonical CSV object so commit/readTable can materialize even when
        # callers upload arbitrary CSV file paths instead of source.csv.
        if file_path.lower().endswith(".csv") and file_path.lower() != "source.csv":
            canonical_key = f"{dataset.db_name}/{dataset.dataset_id}/{dataset.name}/source.csv"
            await storage_service.save_bytes(repo, f"{branch}/{canonical_key}", body_bytes, content_type=content_type)
    except Exception as exc:
        logger.error("Failed to upload file: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="FileUploadFailed", parameters={"message": str(exc)})

    return JSONResponse(content={
        "path": file_path,
        "sizeBytes": len(body_bytes),
        "transactionRid": None,
        "updatedTime": utcnow().isoformat(),
    })


# ---------------------------------------------------------------------------
# Read table
# ---------------------------------------------------------------------------

@router.post(
    "/{datasetRid}/readTable",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("foundry_datasets_v2.read_table")
async def read_table_v2(
    datasetRid: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> JSONResponse:
    """POST /v2/datasets/{datasetRid}/readTable — Read table rows."""
    dataset_id = _dataset_id_from_rid(datasetRid)
    if not dataset_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidDatasetRid", parameters={"datasetRid": datasetRid})

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="DatasetNotFound", parameters={"datasetRid": datasetRid})

    try:
        body = await request.json()
    except Exception:
        body = {}

    requested_columns: list[str] | None = body.get("columns")
    row_limit: int = int(body.get("rowLimit") or 100)
    row_limit = max(1, min(row_limit, 10000))

    # Load latest version
    version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
    if not version:
        try:
            await dataset_registry.reconcile_ingest_state(
                stale_after_seconds=60,
                limit=50,
                use_lock=False,
            )
            version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
        except Exception as reconcile_exc:
            logger.warning("Dataset ingest reconcile before readTable failed: %s", reconcile_exc)
    if not version:
        return JSONResponse(content={"columns": [], "rows": [], "totalRowCount": 0})

    # Try loading sample from cached sample_json first
    sample = version.sample_json or {}
    cached_columns = sample.get("columns") or []
    cached_rows = sample.get("rows") or []

    if cached_rows:
        # Use cached sample data
        columns_out = cached_columns
        if requested_columns and isinstance(requested_columns, list):
            col_indices = []
            col_names = []
            for rc in requested_columns:
                for i, cc in enumerate(cached_columns):
                    name = cc.get("name") if isinstance(cc, dict) else str(cc)
                    if name == rc:
                        col_indices.append(i)
                        col_names.append(cc)
                        break
            columns_out = col_names
            rows_out = []
            for row in cached_rows[:row_limit]:
                if isinstance(row, list):
                    rows_out.append([row[i] if i < len(row) else None for i in col_indices])
                else:
                    rows_out.append(row)
        else:
            rows_out = cached_rows[:row_limit]
            columns_out = cached_columns

        return JSONResponse(content={
            "columns": columns_out,
            "rows": rows_out,
            "totalRowCount": version.row_count or len(cached_rows),
        })

    # Fallback: try loading from lakeFS
    try:
        repo = _resolve_lakefs_raw_repository()
        branch = dataset.branch or "main"
        storage_service = await _get_lakefs_storage(pipeline_registry, request)
        fallback_keys = []
        artifact_key = getattr(version, "artifact_key", None)
        if isinstance(artifact_key, str) and artifact_key.strip():
            fallback_keys.append(_normalize_dataset_object_key(dataset, artifact_key))
        fallback_keys.append(_normalize_dataset_object_key(dataset, "source.csv"))

        for object_key in dict.fromkeys(fallback_keys):
            try:
                csv_bytes = await storage_service.load_bytes(repo, f"{branch}/{object_key}")
            except (FileNotFoundError, LakeFSNotFoundError):
                continue

            reader = csv.reader(io.StringIO(csv_bytes.decode("utf-8-sig", errors="replace")))
            all_rows = list(reader)
            if not all_rows:
                continue
            header = _normalized_csv_headers([str(cell) for cell in all_rows[0]])
            data_rows = all_rows[1: 1 + row_limit]
            columns_out = [{"name": h, "type": "string"} for h in header]
            if requested_columns and isinstance(requested_columns, list):
                col_indices: list[int] = []
                columns_out = []
                for requested_name in [str(col) for col in requested_columns]:
                    for idx, name in enumerate(header):
                        if name == requested_name:
                            col_indices.append(idx)
                            columns_out.append({"name": header[idx], "type": "string"})
                            break
                projected_rows: list[list[Any]] = []
                for row in data_rows:
                    projected_rows.append([row[idx] if idx < len(row) else None for idx in col_indices])
                data_rows = projected_rows

            return JSONResponse(content={
                "columns": columns_out,
                "rows": data_rows,
                "totalRowCount": len(all_rows) - 1,
            })
        return JSONResponse(content={"columns": [], "rows": [], "totalRowCount": 0})
    except (FileNotFoundError, LakeFSNotFoundError):
        # Dataset can legitimately exist before any source object is materialized.
        return JSONResponse(content={"columns": [], "rows": [], "totalRowCount": 0})
    except LakeFSError as exc:
        logger.error("lakeFS readTable load failed: %s", exc)
        return _foundry_error(
            503,
            error_code="SERVICE_UNAVAILABLE",
            error_name="DatasetReadUnavailable",
            parameters={"datasetRid": datasetRid, "message": "lakeFS read failed"},
        )
    except Exception as exc:
        logger.error("Unexpected readTable load failure: %s", exc)
        return _foundry_error(
            500,
            error_code="INTERNAL",
            error_name="DatasetReadFailed",
            parameters={"datasetRid": datasetRid, "message": str(exc)},
        )
