"""
Foundry v2 Attachment Property Router (OMS).

Provides Foundry Attachment Property API v2-compatible endpoints for
uploading, listing, and downloading attachment files via S3/MinIO.

Endpoints:
  POST /attachments/upload
  GET  /{objectType}/{primaryKey}/attachments/{property}
  GET  /{objectType}/{primaryKey}/attachments/{property}/{attachmentRid}
  GET  /{objectType}/{primaryKey}/attachments/{property}/content
  GET  /{objectType}/{primaryKey}/attachments/{property}/{attachmentRid}/content
"""

import json
import logging
import mimetypes
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse, Response, StreamingResponse

from shared.config.search_config import get_instances_index_name
from shared.dependencies.providers import ElasticsearchServiceDep, StorageServiceDep
from shared.observability.tracing import trace_endpoint
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_class_id,
    validate_db_name,
)

logger = logging.getLogger(__name__)

_ATTACHMENTS_BUCKET = "attachments-data"

# Two separate routers: one for upload (no object context), one for property reads.
attachments_upload_router = APIRouter(
    prefix="/v2/ontologies",
    tags=["Foundry Attachments v2"],
)

attachments_property_router = APIRouter(
    prefix="/v2/ontologies/{ontology}/objects",
    tags=["Foundry Attachment Properties v2"],
)


# ---------------------------------------------------------------------------
# Error helpers (mirrors query.py pattern)
# ---------------------------------------------------------------------------


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Optional[Dict[str, Any]] = None,
) -> JSONResponse:
    payload = {
        "errorCode": error_code,
        "errorName": error_name,
        "errorInstanceId": str(uuid4()),
        "parameters": parameters or {},
    }
    return JSONResponse(status_code=status_code, content=payload)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_attachment_rid() -> str:
    return f"ri.attachments.main.attachment.{uuid4()}"


def _detect_media_type(filename: str) -> str:
    """Guess MIME type from filename extension."""
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"


def _attachment_s3_key(attachment_rid: str, filename: str) -> str:
    """Build S3 key for an attachment file."""
    return f"{attachment_rid}/{filename}"


def _parse_attachment_metadata(value: Any) -> List[Dict[str, Any]]:
    """Parse attachment metadata from an ES property value.

    The value may be:
    - A JSON string containing a single attachment dict
    - A JSON string containing a list of attachment dicts
    - A dict directly
    - A list of dicts directly
    """
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return []

    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _find_attachment_by_rid(
    attachments: List[Dict[str, Any]],
    rid: str,
) -> Optional[Dict[str, Any]]:
    """Find an attachment by its RID."""
    for att in attachments:
        if str(att.get("rid") or "").strip() == rid:
            return att
    return None


def _attachment_v2_response(att: Dict[str, Any]) -> Dict[str, Any]:
    """Format a single AttachmentV2 response."""
    return {
        "rid": str(att.get("rid") or ""),
        "filename": str(att.get("filename") or ""),
        "sizeBytes": att.get("sizeBytes") or att.get("size_bytes") or 0,
        "mediaType": str(att.get("mediaType") or att.get("media_type") or "application/octet-stream"),
    }


async def _find_instance_by_pk(
    es: ElasticsearchServiceDep,
    *,
    db_name: str,
    branch: str,
    object_type: str,
    primary_key: str,
) -> Optional[Dict[str, Any]]:
    """Resolve a single instance by its primary key via ES term search."""
    index_name = get_instances_index_name(db_name, branch=branch)
    result = await es.search(
        index=index_name,
        query={
            "bool": {
                "must": [
                    {"term": {"class_id": object_type}},
                    {"term": {"instance_id": primary_key}},
                ]
            }
        },
        size=1,
    )
    hits = result.get("hits", [])
    if isinstance(hits, list) and hits:
        return hits[0] if isinstance(hits[0], dict) else None
    return None


def _extract_attachment_property(source: Dict[str, Any], property_name: str) -> Optional[Any]:
    """Extract attachment property value from an instance ES document.

    Returns the raw value (string/dict/list) of the attachment property,
    or None if the property is not found / not an attachment type.
    """
    properties = source.get("properties")
    if isinstance(properties, list):
        for prop in properties:
            if not isinstance(prop, dict):
                continue
            name = str(prop.get("name") or "").strip()
            prop_type = str(prop.get("type") or "").strip().lower()
            if name == property_name and prop_type in {"attachment", "media"}:
                return prop.get("value")

    # Also check the unindexed data field.
    data = source.get("data")
    if isinstance(data, dict) and property_name in data:
        return data[property_name]
    return None


# ---------------------------------------------------------------------------
# Upload endpoint (no object context)
# ---------------------------------------------------------------------------


@attachments_upload_router.post("/attachments/upload")
@trace_endpoint("upload_attachment")
async def upload_attachment(
    request: Request,
    filename: str = Query(..., description="The name of the file being uploaded"),
    storage: StorageServiceDep = ...,  # type: ignore[assignment]
) -> JSONResponse:
    """Upload a file as a temporary attachment.

    Attachments not linked to objects via actions within one hour are
    automatically removed (per Foundry specification).
    """
    body = await request.body()
    if not body:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="EmptyAttachment",
            parameters={"message": "Request body is empty"},
        )

    rid = _generate_attachment_rid()
    media_type = _detect_media_type(filename)
    s3_key = _attachment_s3_key(rid, filename)

    try:
        await storage.save_bytes(
            bucket=_ATTACHMENTS_BUCKET,
            key=s3_key,
            data=body,
            content_type=media_type,
            metadata={
                "attachment-rid": rid,
                "original-filename": filename,
            },
        )
    except Exception as exc:
        logger.error("Failed to upload attachment to S3: %s", exc, exc_info=True)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="AttachmentUploadFailed",
            parameters={"message": "Failed to store attachment"},
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "rid": rid,
            "filename": filename,
            "sizeBytes": len(body),
            "mediaType": media_type,
        },
    )


# ---------------------------------------------------------------------------
# Attachment Property read endpoints
# ---------------------------------------------------------------------------


@attachments_property_router.get(
    "/{objectType}/{primaryKey}/attachments/{property}",
)
@trace_endpoint("list_property_attachments")
async def list_property_attachments(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    branch: str = Query("main"),
    es: ElasticsearchServiceDep = ...,  # type: ignore[assignment]
) -> JSONResponse:
    """List attachment metadata for a property (single or multiple)."""
    try:
        db_name = validate_db_name(ontology)
        object_type = validate_class_id(objectType)
        branch = validate_branch_name(branch)
    except (SecurityViolationError, ValueError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    source = await _find_instance_by_pk(
        es, db_name=db_name, branch=branch, object_type=object_type, primary_key=primaryKey,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="ObjectNotFound",
            error_name="ObjectNotFound",
            parameters={"objectType": object_type, "primaryKey": primaryKey},
        )

    raw_value = _extract_attachment_property(source, property)
    if raw_value is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="PropertyNotFound",
            error_name="AttachmentPropertyNotFound",
            parameters={"objectType": object_type, "property": property},
        )

    attachments = _parse_attachment_metadata(raw_value)

    if len(attachments) <= 1:
        single = attachments[0] if attachments else {}
        return JSONResponse(content={
            "type": "single",
            **_attachment_v2_response(single),
        })

    return JSONResponse(content={
        "type": "multiple",
        "data": [_attachment_v2_response(a) for a in attachments],
    })


@attachments_property_router.get(
    "/{objectType}/{primaryKey}/attachments/{property}/content",
)
@trace_endpoint("get_attachment_property_content")
async def get_attachment_content(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    branch: str = Query("main"),
    es: ElasticsearchServiceDep = ...,  # type: ignore[assignment]
    storage: StorageServiceDep = ...,  # type: ignore[assignment]
) -> Response:
    """Get the content of a single-valued attachment property."""
    try:
        db_name = validate_db_name(ontology)
        object_type = validate_class_id(objectType)
        branch = validate_branch_name(branch)
    except (SecurityViolationError, ValueError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    source = await _find_instance_by_pk(
        es, db_name=db_name, branch=branch, object_type=object_type, primary_key=primaryKey,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="ObjectNotFound",
            error_name="ObjectNotFound",
            parameters={"objectType": object_type, "primaryKey": primaryKey},
        )

    raw_value = _extract_attachment_property(source, property)
    if raw_value is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="PropertyNotFound",
            error_name="AttachmentPropertyNotFound",
            parameters={"objectType": object_type, "property": property},
        )

    attachments = _parse_attachment_metadata(raw_value)
    if not attachments:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="AttachmentNotFound",
            error_name="AttachmentNotFound",
            parameters={"property": property},
        )

    att = attachments[0]
    rid = str(att.get("rid") or "")
    filename = str(att.get("filename") or "file")
    media_type = str(att.get("mediaType") or att.get("media_type") or "application/octet-stream")

    s3_key = _attachment_s3_key(rid, filename)
    try:
        data = await storage.load_bytes(_ATTACHMENTS_BUCKET, s3_key)
    except FileNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="AttachmentContentNotFound",
            error_name="AttachmentContentNotFound",
            parameters={"attachmentRid": rid},
        )

    return Response(content=data, media_type=media_type)


@attachments_property_router.get(
    "/{objectType}/{primaryKey}/attachments/{property}/{attachmentRid}",
)
@trace_endpoint("get_attachment_by_rid")
async def get_attachment_by_rid(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    attachmentRid: str,
    branch: str = Query("main"),
    es: ElasticsearchServiceDep = ...,  # type: ignore[assignment]
) -> JSONResponse:
    """Get metadata for a single attachment by its RID."""
    try:
        db_name = validate_db_name(ontology)
        object_type = validate_class_id(objectType)
        branch = validate_branch_name(branch)
    except (SecurityViolationError, ValueError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    source = await _find_instance_by_pk(
        es, db_name=db_name, branch=branch, object_type=object_type, primary_key=primaryKey,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="ObjectNotFound",
            error_name="ObjectNotFound",
            parameters={"objectType": object_type, "primaryKey": primaryKey},
        )

    raw_value = _extract_attachment_property(source, property)
    if raw_value is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="PropertyNotFound",
            error_name="AttachmentPropertyNotFound",
            parameters={"objectType": object_type, "property": property},
        )

    attachments = _parse_attachment_metadata(raw_value)
    att = _find_attachment_by_rid(attachments, attachmentRid)
    if att is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="AttachmentNotFound",
            error_name="AttachmentNotFound",
            parameters={"attachmentRid": attachmentRid},
        )

    return JSONResponse(content=_attachment_v2_response(att))


@attachments_property_router.get(
    "/{objectType}/{primaryKey}/attachments/{property}/{attachmentRid}/content",
)
@trace_endpoint("get_attachment_content_by_rid")
async def get_attachment_content_by_rid(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    attachmentRid: str,
    branch: str = Query("main"),
    es: ElasticsearchServiceDep = ...,  # type: ignore[assignment]
    storage: StorageServiceDep = ...,  # type: ignore[assignment]
) -> Response:
    """Get the content of an attachment by its RID."""
    try:
        db_name = validate_db_name(ontology)
        object_type = validate_class_id(objectType)
        branch = validate_branch_name(branch)
    except (SecurityViolationError, ValueError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    source = await _find_instance_by_pk(
        es, db_name=db_name, branch=branch, object_type=object_type, primary_key=primaryKey,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="ObjectNotFound",
            error_name="ObjectNotFound",
            parameters={"objectType": object_type, "primaryKey": primaryKey},
        )

    raw_value = _extract_attachment_property(source, property)
    if raw_value is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="PropertyNotFound",
            error_name="AttachmentPropertyNotFound",
            parameters={"objectType": object_type, "property": property},
        )

    attachments = _parse_attachment_metadata(raw_value)
    att = _find_attachment_by_rid(attachments, attachmentRid)
    if att is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="AttachmentNotFound",
            error_name="AttachmentNotFound",
            parameters={"attachmentRid": attachmentRid},
        )

    filename = str(att.get("filename") or "file")
    media_type = str(att.get("mediaType") or att.get("media_type") or "application/octet-stream")
    s3_key = _attachment_s3_key(attachmentRid, filename)

    try:
        data = await storage.load_bytes(_ATTACHMENTS_BUCKET, s3_key)
    except FileNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="AttachmentContentNotFound",
            error_name="AttachmentContentNotFound",
            parameters={"attachmentRid": attachmentRid},
        )

    return Response(content=data, media_type=media_type)
