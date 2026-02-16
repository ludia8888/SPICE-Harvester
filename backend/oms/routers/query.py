"""
Foundry-style Object Search Router (OMS).

Provides Foundry Search Objects API v2-compatible read surface on top of the
instances Elasticsearch index.
"""

import logging
import re
from hashlib import sha256
from json import dumps as json_dumps
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from oms.dependencies import ValidatedDatabaseName
from shared.config.search_config import get_instances_index_name
from shared.dependencies.providers import ElasticsearchServiceDep
from shared.observability.tracing import trace_endpoint
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_class_id,
)
from shared.utils.foundry_page_token import decode_offset_page_token, encode_offset_page_token

logger = logging.getLogger(__name__)

router = APIRouter()

# Foundry docs: json queries can be nested at most three levels deep.
# Internal depth starts at 0 for root, so max allowed depth index is 2.
_MAX_QUERY_DEPTH = 2
_FIELD_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.-]*$")
_FORBIDDEN_FIELD_SEGMENTS = frozenset(
    {
        "__proto__",
        "constructor",
        "_id",
        "_index",
        "_source",
        "_routing",
        "_type",
        "_version",
        "_seq_no",
        "_primary_term",
    }
)
_TOP_LEVEL_FIELDS = frozenset(
    {
        "instance_id",
        "class_id",
        "class_label",
        "db_name",
        "branch",
        "created_at",
        "updated_at",
        "lifecycle_id",
        "event_id",
        "event_sequence",
        "event_timestamp",
    }
)


class SearchJsonQueryV2(BaseModel):
    """
    Foundry SearchJsonQueryV2-compatible DSL (subset used by this service).
    """

    type: Literal[
        "eq",
        "gt",
        "lt",
        "gte",
        "lte",
        "isNull",
        "contains",
        "containsAnyTerm",
        "containsAllTerms",
        "containsAllTermsInOrder",
        "containsAllTermsInOrderPrefixLastTerm",
        "and",
        "or",
        "not",
    ]
    field: Optional[str] = None
    value: Any = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "SearchJsonQueryV2":
        if self.type in {"and", "or"}:
            if not isinstance(self.value, list) or not self.value:
                raise ValueError(f"{self.type} query requires a non-empty list in value")
            return self

        if self.type == "not":
            if self.value is None:
                raise ValueError("not query requires value")
            return self

        if not isinstance(self.field, str) or not self.field.strip():
            raise ValueError(f"{self.type} query requires field")

        if self.type == "isNull":
            if self.value is not None and not isinstance(self.value, bool):
                raise ValueError("isNull query value must be a boolean when provided")
            return self

        if self.value is None:
            raise ValueError(f"{self.type} query requires value")

        if self.type in {
            "containsAnyTerm",
            "containsAllTerms",
            "containsAllTermsInOrder",
            "containsAllTermsInOrderPrefixLastTerm",
        }:
            if not isinstance(self.value, str) or not self.value.strip():
                raise ValueError(f"{self.type} query requires a non-empty string value")

        return self


class SearchObjectsRequestV2(BaseModel):
    where: Optional[SearchJsonQueryV2] = None
    orderBy: Optional[Dict[str, Any]] = None
    pageSize: int = Field(default=100, ge=1, le=1000)
    pageToken: Optional[str] = None
    select: Optional[List[str]] = None
    selectV2: Optional[List[Any]] = None
    excludeRid: Optional[bool] = None
    snapshot: Optional[bool] = None

    @field_validator("select")
    @classmethod
    def _validate_select(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        cleaned: List[str] = []
        for raw in value:
            field = str(raw or "").strip()
            if not field:
                raise ValueError("select entries must be non-empty strings")
            cleaned.append(field)
        return cleaned

    @model_validator(mode="after")
    def _validate_select_compat(self) -> "SearchObjectsRequestV2":
        if self.select and self.selectV2:
            raise ValueError("Only one of select/selectV2 may be provided")
        return self


class SearchObjectsResponseV2(BaseModel):
    data: List[Dict[str, Any]]
    nextPageToken: Optional[str] = None
    totalCount: str


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


def _validate_field_name(field: str) -> str:
    normalized = str(field or "").strip()
    if not normalized or not _FIELD_NAME_RE.match(normalized):
        raise ValueError(f"Invalid field name: {field!r}")

    for segment in normalized.split("."):
        lowered = segment.lower()
        if segment.startswith("_") or lowered in _FORBIDDEN_FIELD_SEGMENTS:
            raise ValueError(f"Forbidden field name segment: {segment!r}")
    return normalized


def _resolve_field_path(field: str) -> str:
    validated = _validate_field_name(field)
    if validated in _TOP_LEVEL_FIELDS:
        return validated
    if validated.startswith("data.") or validated.startswith("properties."):
        return validated
    return f"data.{validated}"


def _decode_page_token(page_token: Optional[str], *, scope: Optional[str] = None) -> int:
    return decode_offset_page_token(page_token, ttl_seconds=60, expected_scope=scope)


def _encode_page_token(offset: int, *, scope: Optional[str] = None) -> str:
    return encode_offset_page_token(offset, scope=scope)


def _pagination_scope_for_search(
    *,
    db_name: str,
    object_type: str,
    branch: str,
    request: SearchObjectsRequestV2,
) -> str:
    payload = {
        "api": "v2/searchObjects",
        "db": db_name,
        "objectType": object_type,
        "branch": branch,
        "pageSize": int(request.pageSize),
        "where": request.where.model_dump(mode="json") if request.where is not None else None,
        "orderBy": request.orderBy,
        "select": request.select,
        "selectV2": request.selectV2,
        "excludeRid": request.excludeRid,
        "snapshot": request.snapshot,
    }
    canonical = json_dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    fingerprint = sha256(canonical.encode("utf-8")).hexdigest()
    return f"v2/searchObjects|{db_name}|{branch}|{object_type}|{fingerprint}"


def _coerce_query(value: Any) -> SearchJsonQueryV2:
    if isinstance(value, SearchJsonQueryV2):
        return value
    return SearchJsonQueryV2.model_validate(value)


def _default_match_all_where() -> Dict[str, Any]:
    # SearchJsonQueryV2 has no explicit match-all operator.
    return {"type": "not", "value": {"type": "isNull", "field": "instance_id"}}


def _normalize_sort_direction(value: Any) -> str:
    direction = str(value or "asc").strip().lower()
    if direction not in {"asc", "desc"}:
        raise ValueError("orderBy.fields[].direction must be 'asc' or 'desc'")
    return direction


def _resolve_select_fields(request: SearchObjectsRequestV2) -> Optional[List[str]]:
    if request.select:
        return list(request.select)

    if not request.selectV2:
        return None

    extracted: List[str] = []
    for raw in request.selectV2:
        candidate: Optional[str] = None
        if isinstance(raw, str):
            candidate = raw
        elif isinstance(raw, dict):
            candidate = (
                raw.get("propertyApiName")
                or raw.get("apiName")
                or raw.get("name")
            )
            if candidate is None and isinstance(raw.get("property"), dict):
                nested = raw["property"]
                candidate = (
                    nested.get("propertyApiName")
                    or nested.get("apiName")
                    or nested.get("name")
                )

        field = str(candidate or "").strip()
        if not field:
            raise ValueError("selectV2 entries must contain a property identifier")
        extracted.append(field)

    return extracted


def _build_sort_clause(request: SearchObjectsRequestV2) -> List[Dict[str, Any]]:
    order_by = request.orderBy
    if not isinstance(order_by, dict):
        return [{"instance_id": {"order": "asc"}}]

    order_type = str(order_by.get("orderType") or "fields").strip()
    if order_type != "fields":
        raise ValueError("orderBy.orderType must be 'fields'")

    raw_fields = order_by.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise ValueError("orderBy.fields must be a non-empty list")

    sort: List[Dict[str, Any]] = []
    has_instance_id_sort = False

    for item in raw_fields:
        if not isinstance(item, dict):
            raise ValueError("orderBy.fields entries must be objects")
        field = str(item.get("field") or "").strip()
        if not field:
            raise ValueError("orderBy.fields[].field is required")
        direction = _normalize_sort_direction(item.get("direction"))
        field_path = _resolve_field_path(field)
        if field_path == "instance_id":
            has_instance_id_sort = True
        sort.append({field_path: {"order": direction}})

    if not has_instance_id_sort:
        sort.append({"instance_id": {"order": "asc"}})

    return sort


def _to_es_query(query: Any, *, depth: int = 0) -> Dict[str, Any]:
    if depth > _MAX_QUERY_DEPTH:
        raise ValueError("Search query nesting is too deep")

    q = _coerce_query(query)

    if q.type == "and":
        children = [_to_es_query(child, depth=depth + 1) for child in q.value]
        return {"bool": {"must": children}}

    if q.type == "or":
        children = [_to_es_query(child, depth=depth + 1) for child in q.value]
        return {"bool": {"should": children, "minimum_should_match": 1}}

    if q.type == "not":
        child = _to_es_query(q.value, depth=depth + 1)
        return {"bool": {"must_not": [child]}}

    field_path = _resolve_field_path(str(q.field))

    if q.type == "eq":
        return {"term": {field_path: q.value}}
    if q.type == "gt":
        return {"range": {field_path: {"gt": q.value}}}
    if q.type == "lt":
        return {"range": {field_path: {"lt": q.value}}}
    if q.type == "gte":
        return {"range": {field_path: {"gte": q.value}}}
    if q.type == "lte":
        return {"range": {field_path: {"lte": q.value}}}
    if q.type == "isNull":
        is_null = True if q.value is None else bool(q.value)
        if is_null:
            return {"bool": {"must_not": [{"exists": {"field": field_path}}]}}
        return {"exists": {"field": field_path}}

    if q.type == "contains":
        # Foundry semantics: array contains the provided value.
        return {"term": {field_path: q.value}}
    if q.type == "containsAnyTerm":
        return {"match": {field_path: {"query": str(q.value).strip(), "operator": "or"}}}
    if q.type == "containsAllTerms":
        return {"match": {field_path: {"query": str(q.value).strip(), "operator": "and"}}}
    if q.type == "containsAllTermsInOrder":
        return {"match_phrase": {field_path: str(q.value).strip()}}
    if q.type == "containsAllTermsInOrderPrefixLastTerm":
        return {"match_phrase_prefix": {field_path: str(q.value).strip()}}

    raise ValueError(f"Unsupported query operator: {q.type}")


def _flatten_source(source: Dict[str, Any]) -> Dict[str, Any]:
    data = source.get("data")
    flat: Dict[str, Any] = dict(data) if isinstance(data, dict) else {}

    if not flat:
        properties = source.get("properties")
        if isinstance(properties, list):
            for prop in properties:
                if not isinstance(prop, dict):
                    continue
                name = str(prop.get("name") or "").strip()
                if not name:
                    continue
                flat[name] = prop.get("value")

    for key in (
        "instance_id",
        "class_id",
        "class_label",
        "db_name",
        "branch",
        "created_at",
        "updated_at",
    ):
        value = source.get(key)
        if value is not None and key not in flat:
            flat[key] = value

    return flat


@router.post(
    "/objects/{db_name}/{object_type}/search",
    response_model=SearchObjectsResponseV2,
)
@trace_endpoint("oms.query.search_objects_v2")
async def search_objects_v2(
    payload: Dict[str, Any] = Body(...),
    db_name: str = Depends(ValidatedDatabaseName),
    object_type: str = ...,
    branch: str = Query(default="main"),
    es: ElasticsearchServiceDep = ...,
) -> SearchObjectsResponseV2 | JSONResponse:
    """
    Foundry Search Objects API v2-compatible object search.
    """

    try:
        request = SearchObjectsRequestV2.model_validate(payload)
        object_type = validate_class_id(object_type)
        branch = validate_branch_name(branch)
        page_scope = _pagination_scope_for_search(
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            request=request,
        )
        offset = _decode_page_token(request.pageToken, scope=page_scope)

        object_query = _to_es_query(request.where or _default_match_all_where())
        sort_clause = _build_sort_clause(request)
        selected_fields = _resolve_select_fields(request)
        query = {
            "bool": {
                "must": [
                    {"term": {"class_id": object_type}},
                    object_query,
                ]
            }
        }

        index_name = get_instances_index_name(db_name, branch=branch)
        result = await es.search(
            index=index_name,
            query=query,
            size=request.pageSize,
            from_=offset,
            sort=sort_clause,
        )

        hits = result.get("hits", [])
        if not isinstance(hits, list):
            hits = []

        flattened = [_flatten_source(hit) for hit in hits if isinstance(hit, dict)]

        if selected_fields is not None:
            projected: List[Dict[str, Any]] = []
            for row in flattened:
                projected.append({key: row.get(key) for key in selected_fields if key in row})
            flattened = projected

        if bool(request.excludeRid):
            for row in flattened:
                if isinstance(row, dict):
                    row.pop("__rid", None)

        total = int(result.get("total") or 0)

        next_page_token: Optional[str] = None
        next_offset = offset + len(flattened)
        if next_offset < total:
            next_page_token = _encode_page_token(next_offset, scope=page_scope)

        return SearchObjectsResponseV2(
            data=flattened,
            nextPageToken=next_page_token,
            totalCount=str(total),
        )

    except ValidationError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )
    except SecurityViolationError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )
    except ValueError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )
    except Exception as exc:
        logger.error("Foundry object search failed (%s/%s): %s", db_name, object_type, exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="InternalServerError",
            parameters={"message": "Failed to execute object search"},
        )
