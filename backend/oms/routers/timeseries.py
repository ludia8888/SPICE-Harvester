"""
Foundry v2 Time Series Property Router (OMS).

Provides Foundry Time Series Property API v2-compatible endpoints for reading
time series data from S3-backed storage.

Endpoints:
  GET  /{objectType}/{primaryKey}/timeseries/{property}/firstPoint
  GET  /{objectType}/{primaryKey}/timeseries/{property}/lastPoint
  POST /{objectType}/{primaryKey}/timeseries/{property}/streamPoints
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse
from uuid import uuid4

from fastapi import APIRouter, Body, Query, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from shared.config.settings import get_settings
from shared.config.search_config import get_instances_index_name
from shared.dependencies.providers import ElasticsearchServiceDep, StorageServiceDep
from shared.observability.tracing import trace_endpoint
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_class_id,
    validate_db_name,
)

logger = logging.getLogger(__name__)
_ACTOR_HEADER_KEYS = ("X-User-ID", "X-User", "X-Actor")

timeseries_router = APIRouter(
    prefix="/v2/ontologies/{ontology}/objects",
    tags=["Foundry Time Series Property v2"],
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


def _ts_s3_key(
    db_name: str,
    branch: str,
    class_id: str,
    instance_id: str,
    property_name: str,
) -> str:
    """Build canonical S3 key for a time series property."""
    return f"{db_name}/{branch}/{class_id}/{instance_id}/{property_name}.json"


def _timeseries_bucket(storage: StorageServiceDep) -> str:
    configured = getattr(getattr(storage, "_settings", None), "timeseries_bucket", None)
    if isinstance(configured, str) and configured.strip():
        return configured.strip()
    return str(get_settings().storage.timeseries_bucket or "timeseries-data").strip() or "timeseries-data"


def _resolve_timeseries_location(
    *,
    db_name: str,
    branch: str,
    object_type: str,
    primary_key: str,
    property_name: str,
    ts_ref: Optional[str],
    default_bucket: str,
) -> tuple[str, str]:
    canonical_key = _ts_s3_key(db_name, branch, object_type, primary_key, property_name)
    ref = str(ts_ref or "").strip()
    if not ref:
        return default_bucket, canonical_key

    if ref.startswith("s3://"):
        parsed = urlparse(ref)
        bucket = str(parsed.netloc or "").strip() or default_bucket
        key = str(parsed.path or "").lstrip("/")
        return bucket, key or canonical_key

    if "://" in ref:
        return default_bucket, canonical_key

    return default_bucket, ref.lstrip("/") or canonical_key


def _has_actor_headers(request: Request) -> bool:
    return any(str(request.headers.get(key) or "").strip() for key in _ACTOR_HEADER_KEYS)


async def _require_domain_role_if_actor(request: Request, *, db_name: str) -> None:
    if not _has_actor_headers(request):
        return
    await enforce_database_role(
        headers=request.headers,
        db_name=db_name,
        required_roles=DOMAIN_MODEL_ROLES,
        require_env_key="OMS_REQUIRE_DB_ACCESS",
    )


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


def _extract_ts_property(source: Dict[str, Any], property_name: str) -> Optional[str]:
    """Extract time series S3 reference from an instance ES document.

    Returns the S3 key suffix (or full URI) stored in the property value,
    or ``None`` if the property is not found / not a timeseries type.
    """
    properties = source.get("properties")
    if isinstance(properties, list):
        for prop in properties:
            if not isinstance(prop, dict):
                continue
            name = str(prop.get("name") or "").strip()
            prop_type = str(prop.get("type") or "").strip().lower()
            if name == property_name and prop_type in {"timeseries", "time_series"}:
                return str(prop.get("value") or "").strip() or None

    # Also check the unindexed ``data`` field.
    data = source.get("data")
    if isinstance(data, dict) and property_name in data:
        return str(data[property_name] or "").strip() or None
    return None


async def _load_ts_points(
    storage: StorageServiceDep,
    bucket: str,
    key: str,
) -> List[Dict[str, Any]]:
    """Load time series points from S3 JSON file.

    Expected format: ``{"points": [{"time": "ISO8601", "value": ...}, ...]}``
    """
    try:
        data = await storage.load_json(bucket, key)
    except FileNotFoundError:
        return []
    points = data.get("points") if isinstance(data, dict) else None
    if not isinstance(points, list):
        return []
    return [p for p in points if isinstance(p, dict) and "time" in p]


def _filter_points_by_range(
    points: List[Dict[str, Any]],
    range_spec: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Apply optional time range filter to points list."""
    if not range_spec or not isinstance(range_spec, dict):
        return points

    range_type = range_spec.get("type", "absolute")

    if range_type == "absolute":
        start_time = range_spec.get("startTime")
        end_time = range_spec.get("endTime")
        return _filter_absolute(points, start_time, end_time)

    if range_type == "relative":
        return _filter_relative(points, range_spec)

    return points


def _filter_absolute(
    points: List[Dict[str, Any]],
    start_time: Optional[str],
    end_time: Optional[str],
) -> List[Dict[str, Any]]:
    filtered = []
    for p in points:
        t = str(p.get("time") or "")
        if start_time and t < start_time:
            continue
        if end_time and t > end_time:
            continue
        filtered.append(p)
    return filtered


_RELATIVE_UNITS: Dict[str, str] = {
    "MILLISECONDS": "milliseconds",
    "SECONDS": "seconds",
    "MINUTES": "minutes",
    "HOURS": "hours",
    "DAYS": "days",
    "WEEKS": "weeks",
}


def _duration_to_timedelta(duration: Dict[str, Any]) -> timedelta:
    value = int(duration.get("value", 0))
    unit = str(duration.get("unit", "DAYS")).upper()
    if unit == "MONTHS":
        return timedelta(days=value * 30)
    if unit == "YEARS":
        return timedelta(days=value * 365)
    td_unit = _RELATIVE_UNITS.get(unit, "days")
    return timedelta(**{td_unit: value})


def _filter_relative(
    points: List[Dict[str, Any]],
    range_spec: Dict[str, Any],
) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    when = str(range_spec.get("when", "BEFORE")).upper()

    start_dur = range_spec.get("startDuration")
    end_dur = range_spec.get("endDuration")

    if isinstance(start_dur, dict):
        start_delta = _duration_to_timedelta(start_dur)
    else:
        value = int(range_spec.get("value", 0))
        unit = str(range_spec.get("unit", "DAYS")).upper()
        start_delta = _duration_to_timedelta({"value": value, "unit": unit})

    if isinstance(end_dur, dict):
        end_delta = _duration_to_timedelta(end_dur)
    else:
        end_delta = timedelta(0)

    if when == "BEFORE":
        end_time = now - end_delta
        start_time = now - start_delta
    else:
        start_time = now + end_delta
        end_time = now + start_delta

    start_iso = start_time.isoformat()
    end_iso = end_time.isoformat()
    return _filter_absolute(points, start_iso, end_iso)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@timeseries_router.get("/{objectType}/{primaryKey}/timeseries/{property}/firstPoint")
@trace_endpoint("get_timeseries_first_point")
async def get_first_point(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    request: Request,
    branch: str = Query("main"),
    es: ElasticsearchServiceDep = ...,  # type: ignore[assignment]
    storage: StorageServiceDep = ...,  # type: ignore[assignment]
) -> JSONResponse:
    """Get the first (earliest) point of a time series property."""
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
    try:
        await _require_domain_role_if_actor(request, db_name=db_name)
    except ValueError as exc:
        if str(exc).strip().lower() == "permission denied":
            return _foundry_error(
                status.HTTP_403_FORBIDDEN,
                error_code="PERMISSION_DENIED",
                error_name="PermissionDenied",
                parameters={"message": "Permission denied"},
            )
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

    ts_ref = _extract_ts_property(source, property)
    if ts_ref is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="PropertyNotFound",
            error_name="TimeSeriesPropertyNotFound",
            parameters={"objectType": object_type, "property": property},
        )

    bucket, s3_key = _resolve_timeseries_location(
        db_name=db_name,
        branch=branch,
        object_type=object_type,
        primary_key=primaryKey,
        property_name=property,
        ts_ref=ts_ref,
        default_bucket=_timeseries_bucket(storage),
    )
    points = await _load_ts_points(storage, bucket, s3_key)
    if not points:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="TimeSeriesDataNotFound",
            error_name="TimeSeriesDataNotFound",
            parameters={"objectType": object_type, "primaryKey": primaryKey, "property": property},
        )

    return JSONResponse(content=points[0])


@timeseries_router.get("/{objectType}/{primaryKey}/timeseries/{property}/lastPoint")
@trace_endpoint("get_timeseries_last_point")
async def get_last_point(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    request: Request,
    branch: str = Query("main"),
    es: ElasticsearchServiceDep = ...,  # type: ignore[assignment]
    storage: StorageServiceDep = ...,  # type: ignore[assignment]
) -> JSONResponse:
    """Get the last (most recent) point of a time series property."""
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
    try:
        await _require_domain_role_if_actor(request, db_name=db_name)
    except ValueError as exc:
        if str(exc).strip().lower() == "permission denied":
            return _foundry_error(
                status.HTTP_403_FORBIDDEN,
                error_code="PERMISSION_DENIED",
                error_name="PermissionDenied",
                parameters={"message": "Permission denied"},
            )
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

    ts_ref = _extract_ts_property(source, property)
    if ts_ref is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="PropertyNotFound",
            error_name="TimeSeriesPropertyNotFound",
            parameters={"objectType": object_type, "property": property},
        )

    bucket, s3_key = _resolve_timeseries_location(
        db_name=db_name,
        branch=branch,
        object_type=object_type,
        primary_key=primaryKey,
        property_name=property,
        ts_ref=ts_ref,
        default_bucket=_timeseries_bucket(storage),
    )
    points = await _load_ts_points(storage, bucket, s3_key)
    if not points:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="TimeSeriesDataNotFound",
            error_name="TimeSeriesDataNotFound",
            parameters={"objectType": object_type, "primaryKey": primaryKey, "property": property},
        )

    return JSONResponse(content=points[-1])


@timeseries_router.post("/{objectType}/{primaryKey}/timeseries/{property}/streamPoints", response_model=None)
@trace_endpoint("stream_timeseries_points")
async def stream_points(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
    branch: str = Query("main"),
    es: ElasticsearchServiceDep = ...,  # type: ignore[assignment]
    storage: StorageServiceDep = ...,  # type: ignore[assignment]
) -> Union[StreamingResponse, JSONResponse]:
    """Stream all points of a time series property with optional range filter."""
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
    try:
        await _require_domain_role_if_actor(request, db_name=db_name)
    except ValueError as exc:
        if str(exc).strip().lower() == "permission denied":
            return _foundry_error(
                status.HTTP_403_FORBIDDEN,
                error_code="PERMISSION_DENIED",
                error_name="PermissionDenied",
                parameters={"message": "Permission denied"},
            )
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

    ts_ref = _extract_ts_property(source, property)
    if ts_ref is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="PropertyNotFound",
            error_name="TimeSeriesPropertyNotFound",
            parameters={"objectType": object_type, "property": property},
        )

    bucket, s3_key = _resolve_timeseries_location(
        db_name=db_name,
        branch=branch,
        object_type=object_type,
        primary_key=primaryKey,
        property_name=property,
        ts_ref=ts_ref,
        default_bucket=_timeseries_bucket(storage),
    )
    points = await _load_ts_points(storage, bucket, s3_key)

    range_spec = payload.get("range") if isinstance(payload, dict) else None
    if range_spec:
        points = _filter_points_by_range(points, range_spec)

    async def _generate():
        for point in points:
            yield json.dumps(point, default=str) + "\n"

    return StreamingResponse(
        content=_generate(),
        media_type="application/x-ndjson",
    )
