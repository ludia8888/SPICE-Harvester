"""
Foundry-style Object Search Router (OMS).

Provides Foundry Search Objects API v2-compatible read surface on top of the
instances Elasticsearch index.  Includes ES-native aggregate engine.
"""

import logging
import math
import re
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from json import dumps as json_dumps
from typing import Any, Dict, List, Literal, Optional, Tuple
from uuid import uuid4

from fastapi import APIRouter, Body, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from shared.config.search_config import get_instances_index_name
from shared.dependencies.providers import ElasticsearchServiceDep
from shared.observability.tracing import trace_endpoint
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_class_id,
    validate_db_name,
)
from shared.utils.foundry_page_token import decode_offset_page_token, encode_offset_page_token
from shared.utils.instance_properties import flatten_instance_properties
from shared.utils.number_utils import to_int_or_none
from oms.services.ontology_resources import OntologyResourceService

logger = logging.getLogger(__name__)

foundry_router = APIRouter(prefix="/v2/ontologies/{ontology}/objects", tags=["Foundry Object Search v2"])

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
_FOUNDRY_PAGE_TOKEN_TTL_SECONDS = 60 * 60 * 24


class SearchJsonQueryV2(BaseModel):
    """
    Foundry SearchJsonQueryV2-compatible DSL (subset used by this service).
    """

    type: Literal[
        # Comparison
        "eq",
        "gt",
        "lt",
        "gte",
        "lte",
        # Null
        "isNull",
        # Array / set membership
        "contains",
        "in",
        # Text search
        "containsAnyTerm",
        "containsAllTerms",
        "containsAllTermsInOrder",
        "containsAllTermsInOrderPrefixLastTerm",
        "startsWith",
        "wildcard",
        "regex",
        "interval",
        # Date
        "relativeDateRange",
        # Logical
        "and",
        "or",
        "not",
        # Geo-spatial — point queries
        "withinDistanceOf",
        "withinBoundingBox",
        "withinPolygon",
        # Geo-spatial — shape queries
        "intersectsBoundingBox",
        "intersectsPolygon",
        "doesNotIntersectBoundingBox",
        "doesNotIntersectPolygon",
    ]
    field: Optional[str] = None
    value: Any = None
    # ``interval`` uses ``rule`` instead of ``value``
    rule: Any = None

    # ---- Geo-spatial operator sets for validation ----
    _GEO_OPERATORS: frozenset = frozenset({
        "withinDistanceOf",
        "withinBoundingBox",
        "withinPolygon",
        "intersectsBoundingBox",
        "intersectsPolygon",
        "doesNotIntersectBoundingBox",
        "doesNotIntersectPolygon",
    })

    @model_validator(mode="after")
    def _validate_shape(self) -> "SearchJsonQueryV2":
        # -- Logical operators (no field required) -------------------------
        if self.type in {"and", "or"}:
            if not isinstance(self.value, list) or not self.value:
                raise ValueError(f"{self.type} query requires a non-empty list in value")
            return self

        if self.type == "not":
            if self.value is None:
                raise ValueError("not query requires value")
            return self

        # -- All remaining operators require a field -----------------------
        if not isinstance(self.field, str) or not self.field.strip():
            raise ValueError(f"{self.type} query requires field")

        if self.type == "isNull":
            if self.value is not None and not isinstance(self.value, bool):
                raise ValueError("isNull query value must be a boolean when provided")
            return self

        # -- interval uses ``rule`` instead of ``value`` -------------------
        if self.type == "interval":
            if self.rule is None:
                raise ValueError("interval query requires rule")
            return self

        # -- value is required for everything else -------------------------
        if self.value is None:
            raise ValueError(f"{self.type} query requires value")

        # -- String-only operators -----------------------------------------
        if self.type in {
            "containsAnyTerm",
            "containsAllTerms",
            "containsAllTermsInOrder",
            "containsAllTermsInOrderPrefixLastTerm",
            "startsWith",
            "wildcard",
            "regex",
        }:
            if not isinstance(self.value, str) or not self.value.strip():
                raise ValueError(f"{self.type} query requires a non-empty string value")

        # -- ``in`` requires a non-empty list of values --------------------
        if self.type == "in":
            if not isinstance(self.value, list) or not self.value:
                raise ValueError("in query requires a non-empty list in value")

        # -- Date range requires dict --------------------------------------
        if self.type == "relativeDateRange":
            if not isinstance(self.value, dict):
                raise ValueError("relativeDateRange query requires a dict value with startDuration/endDuration")

        # -- Geo operators require dict value (coordinates/geometry) --------
        if self.type in self._GEO_OPERATORS:
            if not isinstance(self.value, dict):
                raise ValueError(f"{self.type} query requires a dict value with geometry/coordinate data")

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


class CountObjectsResponseV2(BaseModel):
    count: Optional[int] = None


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


async def _ensure_object_type_exists(
    *,
    db_name: str,
    branch: str,
    object_type: str,
) -> Optional[JSONResponse]:
    """
    Foundry parity: unknown object types should return 404 (not empty 200).

    We validate existence via the canonical ontology resource registry (Postgres).
    """

    try:
        resources = OntologyResourceService()
        resource = await resources.get_resource(
            db_name,
            branch=branch,
            resource_type="object_type",
            resource_id=object_type,
        )
    except Exception as exc:
        logger.error(
            "Failed to resolve ontology objectType existence (%s/%s@%s): %s",
            db_name,
            object_type,
            branch,
            exc,
            exc_info=True,
        )
        # Search should continue when the ontology registry is unavailable.
        # The ES-backed result set remains a better signal than failing closed
        # on an auxiliary read path.
        return None

    if not resource:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ObjectTypeNotFound",
            parameters={"ontology": db_name, "objectType": object_type, "branch": branch},
        )

    return None


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
    return decode_offset_page_token(
        page_token,
        ttl_seconds=_FOUNDRY_PAGE_TOKEN_TTL_SECONDS,
        expected_scope=scope,
    )


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


def _build_properties_sort_clause(order_by: Any) -> List[Dict[str, Any]]:
    """Fallback sort for indices where ``data`` is not indexed.

    Our instances index uses ``properties`` (nested) for indexed fields and has
    ``data.enabled=false``. Sorting on ``data.<field>`` therefore errors or is
    ignored by Elasticsearch. When that happens, we retry with nested sorts that
    target ``properties.value.numeric``.

    Note: This is a best-effort fallback. It preserves numeric ordering (e.g. price)
    but does not attempt to infer type per property.
    """
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

        if field_path in _TOP_LEVEL_FIELDS:
            if field_path == "instance_id":
                has_instance_id_sort = True
            sort.append({field_path: {"order": direction}})
            continue

        nested = {"path": "properties", "filter": {"term": {"properties.name": field}}}
        sort.append(
            {
                "properties.value.numeric": {
                    "order": direction,
                    "nested": nested,
                    "missing": "_last",
                }
            }
        )

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
    if q.type in {"containsAllTermsInOrderPrefixLastTerm", "startsWith"}:
        return {"match_phrase_prefix": {field_path: str(q.value).strip()}}

    if q.type == "wildcard":
        return {"wildcard": {field_path: {"value": str(q.value).strip()}}}
    if q.type == "regex":
        return {"regexp": {field_path: {"value": str(q.value).strip()}}}
    if q.type == "relativeDateRange":
        return _build_relative_date_range_query(field_path, q.value)

    # -- ``in`` — value is in a set (ES terms query) ----------------------
    if q.type == "in":
        return {"terms": {field_path: q.value}}

    # -- ``interval`` — range sub-rule on analyzed text --------------------
    if q.type == "interval":
        return _build_interval_query(field_path, q.rule)

    # -- Geo-spatial operators ---------------------------------------------
    # Geo queries need to target the indexed geo sub-fields.  For the nested
    # ``properties[]`` structure we wrap in nested → filter(name) → geo query
    # using ``properties.geo_point`` (point queries) or ``properties.geo_shape``
    # (shape queries).  If the field was already resolved to a top-level field
    # that happens to be geo-typed, we use it directly.
    _GEO_POINT_OPS = {"withinDistanceOf", "withinBoundingBox", "withinPolygon"}
    _GEO_SHAPE_OPS = {
        "intersectsBoundingBox", "intersectsPolygon",
        "doesNotIntersectBoundingBox", "doesNotIntersectPolygon",
    }
    if q.type in _GEO_POINT_OPS or q.type in _GEO_SHAPE_OPS:
        return _wrap_geo_query_nested(
            field_name=str(q.field),
            field_path=field_path,
            query_type=q.type,
            value=q.value,
        )

    raise ValueError(f"Unsupported query operator: {q.type}")


def _property_exact_term_clause(value: Any) -> Dict[str, Any]:
    if isinstance(value, bool):
        return {"term": {"properties.value.keyword": str(value).lower()}}
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return {"term": {"properties.value.numeric": float(value)}}
    return {"term": {"properties.value.keyword": str(value)}}


def _property_range_clause(*, op: str, value: Any) -> Dict[str, Any]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return {"range": {"properties.value.numeric": {op: float(value)}}}
    return {"range": {"properties.value.keyword": {op: str(value)}}}


def _to_es_properties_query(query: Any, *, depth: int = 0) -> Dict[str, Any]:
    if depth > _MAX_QUERY_DEPTH:
        raise ValueError("Search query nesting is too deep")

    q = _coerce_query(query)

    if q.type == "and":
        children = [_to_es_properties_query(child, depth=depth + 1) for child in q.value]
        return {"bool": {"must": children}}

    if q.type == "or":
        children = [_to_es_properties_query(child, depth=depth + 1) for child in q.value]
        return {"bool": {"should": children, "minimum_should_match": 1}}

    if q.type == "not":
        child = _to_es_properties_query(q.value, depth=depth + 1)
        return {"bool": {"must_not": [child]}}

    field_name = _validate_field_name(str(q.field or ""))
    field_path = _resolve_field_path(field_name)
    # Top-level fields are already queryable in the canonical path.
    if field_path in _TOP_LEVEL_FIELDS:
        return _to_es_query(q, depth=depth)

    property_name_clause = {"term": {"properties.name": field_name}}
    value_clause: Optional[Dict[str, Any]] = None

    if q.type == "isNull":
        is_null = True if q.value is None else bool(q.value)
        name_query = {"nested": {"path": "properties", "query": property_name_clause}}
        if is_null:
            empty_value = {
                "nested": {
                    "path": "properties",
                    "query": {"bool": {"must": [property_name_clause, {"term": {"properties.value.keyword": ""}}]}},
                }
            }
            none_value = {
                "nested": {
                    "path": "properties",
                    "query": {"bool": {"must": [property_name_clause, {"term": {"properties.value.keyword": "None"}}]}},
                }
            }
            missing_property = {"bool": {"must_not": [name_query]}}
            return {
                "bool": {
                    "should": [missing_property, empty_value, none_value],
                    "minimum_should_match": 1,
                }
            }
        return {
            "nested": {
                "path": "properties",
                "query": {
                    "bool": {
                        "must": [
                            property_name_clause,
                        ],
                        "must_not": [
                            {"term": {"properties.value.keyword": ""}},
                            {"term": {"properties.value.keyword": "None"}},
                        ],
                    }
                },
            }
        }

    if q.type == "eq":
        value_clause = _property_exact_term_clause(q.value)
    elif q.type == "gt":
        value_clause = _property_range_clause(op="gt", value=q.value)
    elif q.type == "lt":
        value_clause = _property_range_clause(op="lt", value=q.value)
    elif q.type == "gte":
        value_clause = _property_range_clause(op="gte", value=q.value)
    elif q.type == "lte":
        value_clause = _property_range_clause(op="lte", value=q.value)
    elif q.type == "containsAnyTerm":
        value_clause = {"match": {"properties.value": {"query": str(q.value).strip(), "operator": "or"}}}
    elif q.type == "containsAllTerms":
        value_clause = {"match": {"properties.value": {"query": str(q.value).strip(), "operator": "and"}}}
    elif q.type == "startsWith":
        value_clause = {"prefix": {"properties.value.keyword": str(q.value).strip()}}
    elif q.type == "in":
        raw_values = q.value if isinstance(q.value, list) else []
        values = [str(item) for item in raw_values]
        value_clause = {"terms": {"properties.value.keyword": values}}

    if value_clause is None:
        # Unsupported operator for properties fallback.
        return _to_es_query(q, depth=depth)

    return {
        "nested": {
            "path": "properties",
            "query": {
                "bool": {
                    "must": [
                        property_name_clause,
                        value_clause,
                    ]
                }
            },
        }
    }


# ---------------------------------------------------------------------------
# Interval query builder
# ---------------------------------------------------------------------------


def _build_interval_query(field_path: str, rule: Any) -> Dict[str, Any]:
    """
    Convert a Foundry ``interval`` query to an ES intervals query.

    Foundry ``rule`` shape:
        {"match": {"query": "...", "maxGaps": 0, "ordered": true}}
        {"allOf": {"intervals": [...], "maxGaps": 0, "ordered": true}}
        {"anyOf": {"intervals": [...]}}

    Maps to ES `intervals` query.
    """
    if not isinstance(rule, dict):
        raise ValueError("interval query rule must be an object")

    # Simple match rule
    if "match" in rule:
        match_spec = rule["match"]
        if isinstance(match_spec, str):
            return {"intervals": {field_path: {"match": {"query": match_spec}}}}
        if isinstance(match_spec, dict):
            es_match: Dict[str, Any] = {"query": str(match_spec.get("query") or "")}
            if "maxGaps" in match_spec:
                es_match["max_gaps"] = int(match_spec["maxGaps"])
            if match_spec.get("ordered"):
                es_match["ordered"] = True
            return {"intervals": {field_path: {"match": es_match}}}

    # allOf rule
    if "allOf" in rule:
        all_of_spec = rule["allOf"]
        if isinstance(all_of_spec, dict):
            intervals = all_of_spec.get("intervals", [])
            sub_rules = [_build_interval_sub_rule(sub) for sub in intervals]
            es_all: Dict[str, Any] = {"intervals": sub_rules}
            if "maxGaps" in all_of_spec:
                es_all["max_gaps"] = int(all_of_spec["maxGaps"])
            if all_of_spec.get("ordered"):
                es_all["ordered"] = True
            return {"intervals": {field_path: {"all_of": es_all}}}

    # anyOf rule
    if "anyOf" in rule:
        any_of_spec = rule["anyOf"]
        if isinstance(any_of_spec, dict):
            intervals = any_of_spec.get("intervals", [])
            sub_rules = [_build_interval_sub_rule(sub) for sub in intervals]
            return {"intervals": {field_path: {"any_of": {"intervals": sub_rules}}}}

    # Fallback: treat entire rule as a match query
    return {"intervals": {field_path: {"match": {"query": str(rule)}}}}


def _build_interval_sub_rule(sub: Any) -> Dict[str, Any]:
    """Build a single interval sub-rule for allOf/anyOf compositions."""
    if isinstance(sub, str):
        return {"match": {"query": sub}}
    if isinstance(sub, dict):
        if "match" in sub:
            match_spec = sub["match"]
            if isinstance(match_spec, str):
                return {"match": {"query": match_spec}}
            if isinstance(match_spec, dict):
                es_match: Dict[str, Any] = {"query": str(match_spec.get("query") or "")}
                if "maxGaps" in match_spec:
                    es_match["max_gaps"] = int(match_spec["maxGaps"])
                if match_spec.get("ordered"):
                    es_match["ordered"] = True
                return {"match": es_match}
    return {"match": {"query": str(sub)}}


# ---------------------------------------------------------------------------
# Geo-spatial query builders
# ---------------------------------------------------------------------------


def _extract_geo_point(raw: Any) -> Dict[str, float]:
    """
    Extract a ``{lat, lon}`` pair from various Foundry coordinate formats.

    Supported input shapes:
    - ``{"center": {"type": "Point", "coordinates": [lon, lat]}, "distance": {...}}``
    - ``{"latitude": ..., "longitude": ...}``
    - ``{"lat": ..., "lon": ...}``
    - ``[lon, lat]``  (GeoJSON order)
    """
    if isinstance(raw, dict):
        if "type" in raw and raw.get("type") == "Point":
            coords = raw.get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                return {"lat": float(coords[1]), "lon": float(coords[0])}
        if "latitude" in raw or "lat" in raw:
            lat = raw.get("latitude") or raw.get("lat")
            lon = raw.get("longitude") or raw.get("lon") or raw.get("lng")
            return {"lat": float(lat), "lon": float(lon)}
    if isinstance(raw, (list, tuple)) and len(raw) >= 2:
        return {"lat": float(raw[1]), "lon": float(raw[0])}
    raise ValueError(f"Cannot parse geo point from: {raw!r}")


def _extract_distance(raw: Any) -> str:
    """
    Extract an ES distance string from Foundry distance format.

    Foundry shape: ``{"value": 10, "unit": "KILOMETERS"}``
    ES format: ``"10km"``
    """
    if isinstance(raw, (int, float)):
        return f"{raw}m"
    if isinstance(raw, str):
        return raw
    if not isinstance(raw, dict):
        raise ValueError(f"Cannot parse distance from: {raw!r}")
    value = raw.get("value")
    if value is None:
        raise ValueError("distance.value is required")
    unit = str(raw.get("unit") or "METERS").strip().upper()
    _DISTANCE_UNIT_MAP = {
        "METERS": "m",
        "KILOMETERS": "km",
        "MILES": "mi",
        "FEET": "ft",
        "YARDS": "yd",
        "NAUTICAL_MILES": "nmi",
    }
    es_unit = _DISTANCE_UNIT_MAP.get(unit, "m")
    return f"{value}{es_unit}"


def _wrap_geo_query_nested(
    *,
    field_name: str,
    field_path: str,
    query_type: str,
    value: Any,
) -> Dict[str, Any]:
    """
    Wrap a geo query in a nested → filter context for ``properties[]``.

    If the resolved ``field_path`` starts with ``data.``, we use the nested
    ``properties[]`` structure:
        nested(path=properties) → bool.must[
            filter(properties.name=field_name),
            geo_query(properties.geo_point / properties.geo_shape)
        ]

    For top-level geo fields (e.g. ``location``), we use the field path
    directly.
    """
    _POINT_OPS = {"withinDistanceOf", "withinBoundingBox", "withinPolygon"}
    _SHAPE_OPS = {
        "intersectsBoundingBox", "intersectsPolygon",
        "doesNotIntersectBoundingBox", "doesNotIntersectPolygon",
    }

    use_nested = field_path.startswith("data.")
    if use_nested:
        geo_sub_field = "properties.geo_point" if query_type in _POINT_OPS else "properties.geo_shape"
    else:
        geo_sub_field = field_path

    # Build the raw geo ES query
    if query_type == "withinDistanceOf":
        geo_query = _build_geo_distance_query(geo_sub_field, value)
    elif query_type == "withinBoundingBox":
        geo_query = _build_geo_bounding_box_query(geo_sub_field, value)
    elif query_type == "withinPolygon":
        geo_query = _build_geo_polygon_query(geo_sub_field, value, negate=False)
    elif query_type == "intersectsBoundingBox":
        geo_query = _build_geo_shape_bbox_query(geo_sub_field, value, relation="intersects")
    elif query_type == "intersectsPolygon":
        geo_query = _build_geo_shape_polygon_query(geo_sub_field, value, relation="intersects")
    elif query_type == "doesNotIntersectBoundingBox":
        geo_query = _build_geo_shape_bbox_query(geo_sub_field, value, relation="disjoint")
    elif query_type == "doesNotIntersectPolygon":
        geo_query = _build_geo_shape_polygon_query(geo_sub_field, value, relation="disjoint")
    else:
        raise ValueError(f"Unsupported geo operator: {query_type}")

    if not use_nested:
        return geo_query

    # Wrap in nested + name filter
    return {
        "nested": {
            "path": "properties",
            "query": {
                "bool": {
                    "must": [
                        {"term": {"properties.name": field_name}},
                        geo_query,
                    ]
                }
            },
        }
    }


def _build_geo_distance_query(field_path: str, value: Any) -> Dict[str, Any]:
    """
    ``withinDistanceOf`` → ES ``geo_distance`` query.

    Foundry shape: ``{"center": <GeoPoint>, "distance": {"value": 10, "unit": "KILOMETERS"}}``
    """
    if not isinstance(value, dict):
        raise ValueError("withinDistanceOf value must be an object with center and distance")
    center_raw = value.get("center")
    if center_raw is None:
        raise ValueError("withinDistanceOf.center is required")
    center = _extract_geo_point(center_raw)
    distance = _extract_distance(value.get("distance") or value.get("radius"))
    return {
        "geo_distance": {
            "distance": distance,
            field_path: center,
        }
    }


def _build_geo_bounding_box_query(field_path: str, value: Any) -> Dict[str, Any]:
    """
    ``withinBoundingBox`` → ES ``geo_bounding_box`` query.

    Foundry shape: ``{"topLeft": <GeoPoint>, "bottomRight": <GeoPoint>}``
    """
    if not isinstance(value, dict):
        raise ValueError("withinBoundingBox value must be an object with topLeft and bottomRight")
    top_left_raw = value.get("topLeft") or value.get("top_left")
    bottom_right_raw = value.get("bottomRight") or value.get("bottom_right")
    if top_left_raw is None or bottom_right_raw is None:
        raise ValueError("withinBoundingBox requires topLeft and bottomRight")
    top_left = _extract_geo_point(top_left_raw)
    bottom_right = _extract_geo_point(bottom_right_raw)
    return {
        "geo_bounding_box": {
            field_path: {
                "top_left": top_left,
                "bottom_right": bottom_right,
            }
        }
    }


def _build_geo_polygon_query(field_path: str, value: Any, *, negate: bool) -> Dict[str, Any]:
    """
    ``withinPolygon`` → ES ``geo_shape`` query with ``within`` relation.

    Foundry shape: ``{"type": "Polygon", "coordinates": [[[lon, lat], ...]]}``
    or ``{"polygon": {"type": "Polygon", "coordinates": ...}}``
    """
    polygon = _extract_polygon_geojson(value)
    query: Dict[str, Any] = {
        "geo_shape": {
            field_path: {
                "shape": polygon,
                "relation": "within",
            }
        }
    }
    if negate:
        return {"bool": {"must_not": [query]}}
    return query


def _build_geo_shape_bbox_query(
    field_path: str, value: Any, *, relation: str
) -> Dict[str, Any]:
    """
    ``intersectsBoundingBox`` / ``doesNotIntersectBoundingBox``
    → ES ``geo_shape`` envelope query.
    """
    if not isinstance(value, dict):
        raise ValueError(f"Bounding box value must be an object, got {type(value).__name__}")
    top_left_raw = value.get("topLeft") or value.get("top_left")
    bottom_right_raw = value.get("bottomRight") or value.get("bottom_right")
    if top_left_raw is None or bottom_right_raw is None:
        raise ValueError("Bounding box requires topLeft and bottomRight")
    tl = _extract_geo_point(top_left_raw)
    br = _extract_geo_point(bottom_right_raw)
    envelope = {
        "type": "envelope",
        "coordinates": [[tl["lon"], tl["lat"]], [br["lon"], br["lat"]]],
    }
    query: Dict[str, Any] = {
        "geo_shape": {
            field_path: {
                "shape": envelope,
                "relation": relation,
            }
        }
    }
    if relation == "disjoint":
        return query
    return query


def _build_geo_shape_polygon_query(
    field_path: str, value: Any, *, relation: str
) -> Dict[str, Any]:
    """
    ``intersectsPolygon`` / ``doesNotIntersectPolygon``
    → ES ``geo_shape`` polygon query.
    """
    polygon = _extract_polygon_geojson(value)
    return {
        "geo_shape": {
            field_path: {
                "shape": polygon,
                "relation": relation,
            }
        }
    }


def _extract_polygon_geojson(value: Any) -> Dict[str, Any]:
    """
    Extract GeoJSON polygon from Foundry value.

    Accepted shapes:
    - ``{"type": "Polygon", "coordinates": [[[lon, lat], ...]]}``
    - ``{"polygon": <GeoJSON>}``
    - ``{"coordinates": [...]}``  (implied Polygon)
    """
    if not isinstance(value, dict):
        raise ValueError("Polygon value must be an object")
    # Direct GeoJSON
    if value.get("type") == "Polygon" and "coordinates" in value:
        return {"type": "Polygon", "coordinates": value["coordinates"]}
    # Wrapped in polygon key
    polygon = value.get("polygon")
    if isinstance(polygon, dict):
        return _extract_polygon_geojson(polygon)
    # Coordinates-only
    if "coordinates" in value:
        return {"type": "Polygon", "coordinates": value["coordinates"]}
    raise ValueError("Cannot extract polygon geometry from value")


_DURATION_UNIT_TO_TIMEDELTA = {
    "SECONDS": lambda v: timedelta(seconds=v),
    "MINUTES": lambda v: timedelta(minutes=v),
    "HOURS": lambda v: timedelta(hours=v),
    "DAYS": lambda v: timedelta(days=v),
    "WEEKS": lambda v: timedelta(weeks=v),
    "MONTHS": lambda v: timedelta(days=v * 30),  # approximate
    "YEARS": lambda v: timedelta(days=v * 365),  # approximate
}


def _parse_duration_to_timedelta(duration: Any) -> timedelta:
    """Convert a Foundry duration dict {value, unit} to a Python timedelta."""
    if not isinstance(duration, dict):
        raise ValueError("Duration must be an object with 'value' and 'unit'")
    raw_value = duration.get("value")
    if raw_value is None:
        raise ValueError("Duration.value is required")
    try:
        numeric_value = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError(f"Duration.value must be numeric, got: {raw_value!r}")
    unit = str(duration.get("unit") or "").strip().upper()
    factory = _DURATION_UNIT_TO_TIMEDELTA.get(unit)
    if factory is None:
        raise ValueError(f"Unsupported duration unit: {unit!r}")
    return factory(numeric_value)


def _build_relative_date_range_query(field_path: str, value: Any) -> Dict[str, Any]:
    """
    Convert a Foundry relativeDateRange query to an ES range query.

    value shape: {
        "startDuration": {"value": 7, "unit": "DAYS"},
        "endDuration":   {"value": 0, "unit": "DAYS"},  // optional
        "reference": "NOW"  // optional, always treated as now
    }
    """
    if not isinstance(value, dict):
        raise ValueError("relativeDateRange value must be an object")

    now = datetime.now(tz=timezone.utc)

    start_duration = value.get("startDuration")
    if start_duration is None:
        raise ValueError("relativeDateRange.startDuration is required")
    start_delta = _parse_duration_to_timedelta(start_duration)
    start_dt = now - start_delta

    end_duration = value.get("endDuration")
    if end_duration is not None:
        end_delta = _parse_duration_to_timedelta(end_duration)
        end_dt = now - end_delta
    else:
        end_dt = now

    # Ensure start <= end
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    return {
        "range": {
            field_path: {
                "gte": start_dt.isoformat(),
                "lte": end_dt.isoformat(),
            }
        }
    }


# ---------------------------------------------------------------------------
# ES-native Aggregate Engine
# ---------------------------------------------------------------------------

_SUPPORTED_METRIC_TYPES = frozenset({
    "count", "sum", "avg", "min", "max",
    "exactDistinct", "approximateDistinct",
    "approximatePercentile",
})

_SUPPORTED_GROUP_BY_TYPES = frozenset({"exact", "fixedWidth", "ranges"})


def _resolve_property_nested_path(field: str) -> str:
    """
    Map a Foundry property field name to the nested properties[] ES path.

    Properties are stored as:
        properties[].name     (keyword)
        properties[].value    (text, with .keyword and .numeric sub-fields)
    """
    validated = _validate_field_name(field)
    return validated


def _metric_agg_key(index: int, clause: Dict[str, Any]) -> str:
    """Stable key for an aggregation bucket/metric."""
    explicit = str(clause.get("name") or "").strip()
    if explicit:
        return f"m_{index}_{explicit}"
    return f"m_{index}"


def _build_metric_agg(clause: Dict[str, Any], index: int) -> Tuple[str, Dict[str, Any]]:
    """
    Build a single ES metric aggregation for a Foundry aggregation clause.
    Returns (agg_key, agg_body) for embedding inside a nested/filter context.
    """
    metric_type = str(clause.get("type") or "").strip()
    if metric_type not in _SUPPORTED_METRIC_TYPES:
        raise ValueError(f"Unsupported aggregation type: {metric_type!r}")

    field = str(clause.get("field") or "").strip() or None
    agg_key = _metric_agg_key(index, clause)

    if metric_type == "count":
        # Document count — no field needed, we use value_count if field given.
        if field:
            return agg_key, {"value_count": {"field": "properties.value.keyword"}}
        # Global count is captured via the nested doc_count.
        return agg_key, {"value_count": {"field": "properties.name"}}

    if field is None:
        raise ValueError(f"{metric_type} aggregation requires a field")

    numeric_field = "properties.value.numeric"
    keyword_field = "properties.value.keyword"

    if metric_type == "sum":
        return agg_key, {"sum": {"field": numeric_field}}
    if metric_type == "avg":
        return agg_key, {"avg": {"field": numeric_field}}
    if metric_type == "min":
        return agg_key, {"min": {"field": numeric_field}}
    if metric_type == "max":
        return agg_key, {"max": {"field": numeric_field}}
    if metric_type in {"exactDistinct", "approximateDistinct"}:
        return agg_key, {"cardinality": {"field": keyword_field}}
    if metric_type == "approximatePercentile":
        percentile = clause.get("approximatePercentile")
        if percentile is None:
            raise ValueError("approximatePercentile aggregation requires approximatePercentile")
        try:
            pct = float(percentile) * 100.0  # Foundry uses 0-1, ES uses 0-100
        except (TypeError, ValueError):
            raise ValueError(f"approximatePercentile must be numeric, got: {percentile!r}")
        pct = max(0.0, min(100.0, pct))
        return agg_key, {"percentiles": {"field": numeric_field, "percents": [pct]}}

    raise ValueError(f"Unsupported aggregation type: {metric_type!r}")


def _wrap_nested_metric(field_name: str, metric_key: str, metric_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Wrap a metric aggregation in a nested→filter context for the properties[] array.

    Pattern:
        nested(path=properties) → filter(properties.name=field) → metric(properties.value.*)
    """
    return {
        "nested": {"path": "properties"},
        "aggs": {
            "field_filter": {
                "filter": {"term": {"properties.name": field_name}},
                "aggs": {
                    metric_key: metric_body,
                },
            },
        },
    }


def _build_es_aggregation(
    aggregation_clauses: List[Dict[str, Any]],
    group_by_clauses: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build the complete ES aggregation dict from Foundry aggregation + groupBy clauses.
    """
    aggs: Dict[str, Any] = {}

    if not group_by_clauses:
        # ------ No groupBy: flat metrics ------
        for idx, clause in enumerate(aggregation_clauses):
            metric_type = str(clause.get("type") or "").strip()
            field = str(clause.get("field") or "").strip() or None
            metric_key, metric_body = _build_metric_agg(clause, idx)

            if metric_type == "count" and not field:
                # Total doc count comes from hits.total, no agg needed.
                # We still emit a dummy so we have a key to reference.
                aggs[f"wrapper_{metric_key}"] = {
                    "nested": {"path": "properties"},
                    "aggs": {
                        metric_key: {"value_count": {"field": "properties.name"}},
                    },
                }
            elif field:
                aggs[f"wrapper_{metric_key}"] = _wrap_nested_metric(field, metric_key, metric_body)
            else:
                raise ValueError(f"{metric_type} aggregation requires a field")
        return aggs

    # ------ With groupBy ------
    # We support a single groupBy dimension for now (Foundry typically uses one).
    # Multiple dimensions are chained.
    if len(group_by_clauses) > 3:
        raise ValueError("At most 3 groupBy dimensions are supported")

    # Build the innermost metric aggs first
    inner_metric_aggs: Dict[str, Any] = {}
    for idx, clause in enumerate(aggregation_clauses):
        metric_type = str(clause.get("type") or "").strip()
        field = str(clause.get("field") or "").strip() or None
        metric_key, metric_body = _build_metric_agg(clause, idx)

        if metric_type == "count" and not field:
            # count without field = bucket doc_count (handled in parse)
            continue

        if field:
            # For metrics on a different field than the groupBy, we need
            # reverse_nested → nested → filter → metric.
            inner_metric_aggs[f"wrapper_{metric_key}"] = {
                "reverse_nested": {},
                "aggs": {
                    f"nested_{metric_key}": _wrap_nested_metric(field, metric_key, metric_body),
                },
            }

    # Build groupBy bucket agg (inside-out for multiple dimensions)
    current_aggs = dict(inner_metric_aggs)

    for dim_idx in reversed(range(len(group_by_clauses))):
        gb = group_by_clauses[dim_idx]
        gb_type = str(gb.get("type") or "").strip()
        if gb_type not in _SUPPORTED_GROUP_BY_TYPES:
            raise ValueError(f"Unsupported groupBy type: {gb_type!r}")

        gb_field = str(gb.get("field") or "").strip()
        if not gb_field:
            raise ValueError("groupBy.field is required")

        bucket_key = f"gb_{dim_idx}"

        if gb_type == "exact":
            max_groups = int(gb.get("maxGroupCount") or 100)
            bucket_agg: Dict[str, Any] = {
                "terms": {
                    "field": "properties.value.keyword",
                    "size": max_groups,
                },
            }
        elif gb_type == "fixedWidth":
            width = gb.get("fixedWidth")
            if width is None:
                raise ValueError("fixedWidth groupBy requires fixedWidth")
            try:
                width_val = float(width)
            except (TypeError, ValueError):
                raise ValueError(f"fixedWidth must be numeric, got: {width!r}")
            bucket_agg = {
                "histogram": {
                    "field": "properties.value.numeric",
                    "interval": width_val,
                    "min_doc_count": 0,
                },
            }
        elif gb_type == "ranges":
            raw_ranges = gb.get("ranges")
            if not isinstance(raw_ranges, list) or not raw_ranges:
                raise ValueError("ranges groupBy requires a non-empty ranges list")
            es_ranges: List[Dict[str, Any]] = []
            for r in raw_ranges:
                if not isinstance(r, dict):
                    raise ValueError("ranges entries must be objects")
                entry: Dict[str, Any] = {}
                if "startValue" in r:
                    entry["from"] = r["startValue"]
                if "endValue" in r:
                    entry["to"] = r["endValue"]
                es_ranges.append(entry)
            bucket_agg = {
                "range": {
                    "field": "properties.value.numeric",
                    "ranges": es_ranges,
                },
            }
        else:
            raise ValueError(f"Unsupported groupBy type: {gb_type!r}")

        if current_aggs:
            bucket_agg["aggs"] = current_aggs

        # Wrap in nested → filter for the groupBy field
        current_aggs = {
            f"nested_{bucket_key}": {
                "nested": {"path": "properties"},
                "aggs": {
                    f"filter_{bucket_key}": {
                        "filter": {"term": {"properties.name": gb_field}},
                        "aggs": {
                            bucket_key: bucket_agg,
                        },
                    },
                },
            },
        }

    aggs = current_aggs
    return aggs


def _extract_metric_value(
    bucket: Dict[str, Any],
    metric_key: str,
    clause: Dict[str, Any],
    *,
    parent_doc_count: int,
) -> Any:
    """Extract a metric value from an ES aggregation bucket."""
    metric_type = str(clause.get("type") or "").strip()
    field = str(clause.get("field") or "").strip() or None

    if metric_type == "count" and not field:
        return parent_doc_count

    # The metric is wrapped: wrapper_{metric_key} → (reverse_nested) → nested_{metric_key} → field_filter → {metric_key}
    # OR for flat (no groupBy): wrapper_{metric_key} → field_filter → {metric_key}
    wrapper = bucket.get(f"wrapper_{metric_key}", {})

    # Grouped path: reverse_nested → nested wrapper
    nested_wrapper = wrapper.get(f"nested_{metric_key}", {})
    if nested_wrapper:
        field_filter = nested_wrapper.get("field_filter", {})
    else:
        # Flat path (no groupBy)
        field_filter = wrapper.get("field_filter", {})

    raw = field_filter.get(metric_key, {})

    if metric_type == "approximatePercentile":
        values = raw.get("values", {})
        # Return the first (and only) percentile value
        for _pct_key, pct_val in values.items():
            if pct_val is not None and not (isinstance(pct_val, float) and math.isnan(pct_val)):
                return pct_val
            return None
        return None

    if metric_type in {"exactDistinct", "approximateDistinct"}:
        return raw.get("value", 0)

    if metric_type == "count":
        return raw.get("value", 0)

    # sum, avg, min, max
    val = raw.get("value")
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    return val


def _metric_display_name(clause: Dict[str, Any], index: int) -> str:
    explicit = str(clause.get("name") or "").strip()
    if explicit:
        return explicit
    metric_type = str(clause.get("type") or f"metric_{index}").strip() or f"metric_{index}"
    return f"{metric_type}_{index}"


def _parse_es_aggregation_response(
    es_aggs: Dict[str, Any],
    aggregation_clauses: List[Dict[str, Any]],
    group_by_clauses: List[Dict[str, Any]],
    *,
    total_count: int,
    accuracy_request: Optional[str],
    include_compute_usage: Any,
) -> Dict[str, Any]:
    """Convert ES aggregation response to Foundry aggregate response format."""

    if not group_by_clauses:
        # Flat metrics (no grouping)
        metrics: List[Dict[str, Any]] = []
        for idx, clause in enumerate(aggregation_clauses):
            metric_key = _metric_agg_key(idx, clause)
            value = _extract_metric_value(
                es_aggs, metric_key, clause, parent_doc_count=total_count,
            )
            metrics.append({"name": _metric_display_name(clause, idx), "value": value})

        data = [{"group": {}, "metrics": metrics}]
    else:
        # Grouped metrics — extract from nested bucket structure
        data = _parse_grouped_buckets(
            es_aggs,
            group_by_clauses=group_by_clauses,
            aggregation_clauses=aggregation_clauses,
            dim_idx=0,
            parent_group={},
        )

    accuracy = "APPROXIMATE" if str(accuracy_request or "").strip() == "ALLOW_APPROXIMATE" else "ACCURATE"
    out: Dict[str, Any] = {
        "accuracy": accuracy,
        "data": data,
        "excludedItems": 0,
    }
    if include_compute_usage:
        out["computeUsage"] = 0
    return out


def _parse_grouped_buckets(
    aggs: Dict[str, Any],
    *,
    group_by_clauses: List[Dict[str, Any]],
    aggregation_clauses: List[Dict[str, Any]],
    dim_idx: int,
    parent_group: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Recursively parse grouped ES aggregation buckets into Foundry data format."""
    gb = group_by_clauses[dim_idx]
    gb_field = str(gb.get("field") or "").strip()
    bucket_key = f"gb_{dim_idx}"

    nested = aggs.get(f"nested_{bucket_key}", {})
    filtered = nested.get(f"filter_{bucket_key}", {})
    bucket_agg = filtered.get(bucket_key, {})
    buckets = bucket_agg.get("buckets", [])

    # Handle range buckets (returned as list) vs terms buckets (also list)
    if isinstance(buckets, dict):
        # ES sometimes returns range buckets as a dict keyed by range name
        buckets = list(buckets.values())

    is_last_dim = dim_idx == len(group_by_clauses) - 1
    results: List[Dict[str, Any]] = []

    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        # Group key: "key" for terms/histogram, "from"-"to" for ranges
        group_value: Any
        if "key" in bucket:
            group_value = bucket["key"]
        elif "from" in bucket or "to" in bucket:
            group_value = f"{bucket.get('from', '*')}-{bucket.get('to', '*')}"
        else:
            continue

        current_group = {**parent_group, gb_field: group_value}
        doc_count = bucket.get("doc_count", 0)

        if is_last_dim:
            # Extract metrics from this bucket
            metrics: List[Dict[str, Any]] = []
            for idx, clause in enumerate(aggregation_clauses):
                metric_key = _metric_agg_key(idx, clause)
                value = _extract_metric_value(
                    bucket, metric_key, clause, parent_doc_count=doc_count,
                )
                metrics.append({"name": _metric_display_name(clause, idx), "value": value})
            results.append({"group": current_group, "metrics": metrics})
        else:
            # Recurse into the next dimension
            sub_results = _parse_grouped_buckets(
                bucket,
                group_by_clauses=group_by_clauses,
                aggregation_clauses=aggregation_clauses,
                dim_idx=dim_idx + 1,
                parent_group=current_group,
            )
            results.extend(sub_results)

    return results


def _parse_aggregation_clauses(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Validate and extract aggregation + groupBy clauses from payload."""
    aggregation = payload.get("aggregation")
    group_by = payload.get("groupBy")
    if not isinstance(aggregation, list) or not aggregation:
        raise ValueError("aggregation is required and must be a non-empty list")
    if not isinstance(group_by, list):
        raise ValueError("groupBy is required and must be a list (can be empty)")

    aggregation_clauses = [clause for clause in aggregation if isinstance(clause, dict)]
    group_by_clauses = [clause for clause in group_by if isinstance(clause, dict)]
    if len(aggregation_clauses) != len(aggregation):
        raise ValueError("aggregation must be an array of objects")
    if len(group_by_clauses) != len(group_by):
        raise ValueError("groupBy must be an array of objects")
    return aggregation_clauses, group_by_clauses


def _flatten_source(source: Dict[str, Any]) -> Dict[str, Any]:
    data = source.get("data")
    flat: Dict[str, Any] = dict(data) if isinstance(data, dict) else {}

    if not flat:
        flat = flatten_instance_properties(source.get("properties"))

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


def _source_instance_key(source: Dict[str, Any]) -> Optional[str]:
    class_id = str(source.get("class_id") or "").strip()
    if not class_id:
        data = source.get("data")
        if isinstance(data, dict):
            class_id = str(data.get("class_id") or "").strip()

    instance_id = str(source.get("instance_id") or "").strip()
    if not instance_id:
        data = source.get("data")
        if isinstance(data, dict):
            instance_id = str(data.get("instance_id") or "").strip()
    if not instance_id:
        return None
    return f"{class_id}|{instance_id}"


def _source_sort_rank(source: Dict[str, Any]) -> tuple[str, int, int]:
    seq = to_int_or_none(source.get("event_sequence"))
    updated_at = str(source.get("updated_at") or "").strip()
    # Overlay/action-projected docs should win tie-breaks when recency is equal.
    overlay_rank = 1 if ("patchset_commit_id" in source or "overlay_tombstone" in source) else 0
    return (updated_at, overlay_rank, int(seq) if seq is not None else -1)


def _collapse_duplicate_sources(sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_key: Dict[str, Dict[str, Any]] = {}
    key_order: List[str] = []
    unkeyed: List[Dict[str, Any]] = []

    for source in sources:
        if not isinstance(source, dict):
            continue
        key = _source_instance_key(source)
        if key is None:
            unkeyed.append(source)
            continue

        existing = by_key.get(key)
        if existing is None:
            by_key[key] = source
            key_order.append(key)
            continue

        if _source_sort_rank(source) >= _source_sort_rank(existing):
            by_key[key] = source

    collapsed: List[Dict[str, Any]] = []
    for key in key_order:
        source = by_key.get(key)
        if not isinstance(source, dict):
            continue
        if bool(source.get("overlay_tombstone")):
            continue
        collapsed.append(source)

    collapsed.extend(unkeyed)
    return collapsed


def _rid_component(value: Any, *, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    normalized = "".join(ch if (ch.isalnum() or ch in {"_", "-", "."}) else "_" for ch in text)
    normalized = normalized.strip("._-")
    return normalized or fallback


def _default_object_rid(*, db_name: str, object_type: str, primary_key: str) -> str:
    return (
        "ri.spice.main.object."
        f"{_rid_component(db_name, fallback='db')}."
        f"{_rid_component(object_type, fallback='objectType')}."
        f"{_rid_component(primary_key, fallback='primaryKey')}"
    )


def _prune_none_values(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, inner in value.items():
            if inner is None:
                continue
            out[key] = _prune_none_values(inner)
        return out
    if isinstance(value, list):
        return [_prune_none_values(item) for item in value if item is not None]
    return value


def _normalize_foundry_object_row(
    *,
    row: Dict[str, Any],
    db_name: str,
    object_type: str,
    exclude_rid: bool,
    ) -> Dict[str, Any]:
    normalized = _prune_none_values(dict(row))
    if not isinstance(normalized, dict):
        normalized = {}

    normalized_object_type = str(
        normalized.get("__apiName")
        or normalized.get("class_id")
        or normalized.get("objectType")
        or object_type
        or ""
    ).strip()
    if normalized_object_type:
        normalized["__apiName"] = normalized_object_type

    primary_key = str(
        normalized.get("__primaryKey")
        or normalized.get("primaryKey")
        or normalized.get("instance_id")
        or normalized.get("id")
        or ""
    ).strip()
    if primary_key:
        normalized["__primaryKey"] = primary_key

    if exclude_rid:
        normalized.pop("__rid", None)
    else:
        rid = str(normalized.get("__rid") or "").strip()
        if not rid and normalized_object_type and primary_key:
            normalized["__rid"] = _default_object_rid(
                db_name=db_name,
                object_type=normalized_object_type,
                primary_key=primary_key,
            )

    # Keep a stable properties object to match Foundry-style object payloads.
    if "properties" not in normalized or not isinstance(normalized.get("properties"), dict):
        metadata_keys = {
            "__rid",
            "__primaryKey",
            "__apiName",
            "primaryKey",
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
            "overlay_tombstone",
            "patchset_commit_id",
        }
        normalized["properties"] = {
            key: value
            for key, value in normalized.items()
            if not key.startswith("__") and key not in metadata_keys and key != "properties"
        }

    return normalized


async def _search_objects_v2_impl(
    payload: Dict[str, Any],
    *,
    db_name: str,
    object_type: str,
    branch: str,
    es: ElasticsearchServiceDep,
) -> SearchObjectsResponseV2 | JSONResponse:
    """
    Foundry Search Objects API v2-compatible object search.
    """

    try:
        request = SearchObjectsRequestV2.model_validate(payload)
        object_type = validate_class_id(object_type)
        branch = validate_branch_name(branch)
        missing_object_type_response = await _ensure_object_type_exists(
            db_name=db_name,
            branch=branch,
            object_type=object_type,
        )
        page_scope = _pagination_scope_for_search(
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            request=request,
        )
        offset = _decode_page_token(request.pageToken, scope=page_scope)

        where_clause = request.where or _default_match_all_where()
        object_query = _to_es_query(where_clause)
        properties_fallback_query = _to_es_properties_query(where_clause)
        sort_clause = _build_sort_clause(request)
        selected_fields = _resolve_select_fields(request)
        index_name = get_instances_index_name(db_name, branch=branch)

        async def _run_search(search_query: Dict[str, Any], *, prefer_properties_sort: bool = False) -> Dict[str, Any]:
            active_sort = (
                _build_properties_sort_clause(request.orderBy)
                if prefer_properties_sort and request.orderBy is not None
                else sort_clause
            )
            try:
                return await es.search(
                    index=index_name,
                    query=search_query,
                    size=request.pageSize,
                    from_=offset,
                    sort=active_sort,
                )
            except Exception:
                # Sort errors are common when callers request orderBy on fields that live under nested ``properties``.
                # Retry with nested properties-based sorting.
                if request.orderBy is None or prefer_properties_sort:
                    raise
                return await es.search(
                    index=index_name,
                    query=search_query,
                    size=request.pageSize,
                    from_=offset,
                    sort=_build_properties_sort_clause(request.orderBy),
                )

        query = {
            "bool": {
                "must": [
                    {"term": {"class_id": object_type}},
                    object_query,
                ]
            }
        }
        result = await _run_search(query)
        if (
            properties_fallback_query != object_query
            and int(result.get("total") or 0) <= 0
        ):
            fallback_query = {
                "bool": {
                    "must": [
                        {"term": {"class_id": object_type}},
                        properties_fallback_query,
                    ]
                }
            }
            result = await _run_search(fallback_query, prefer_properties_sort=True)

        hits = result.get("hits", [])
        if not isinstance(hits, list):
            hits = []

        raw_sources = [hit for hit in hits if isinstance(hit, dict)]
        collapsed_sources = _collapse_duplicate_sources(raw_sources)
        flattened = [_flatten_source(hit) for hit in collapsed_sources]

        if selected_fields is not None:
            projected: List[Dict[str, Any]] = []
            for row in flattened:
                projected_row = {key: row.get(key) for key in selected_fields if key in row}
                for required_field in ("instance_id", "class_id"):
                    if required_field in row:
                        projected_row.setdefault(required_field, row.get(required_field))
                projected.append(projected_row)
            flattened = projected

        normalized_rows: List[Dict[str, Any]] = []
        for row in flattened:
            if not isinstance(row, dict):
                continue
            normalized_rows.append(
                _normalize_foundry_object_row(
                    row=row,
                    db_name=db_name,
                    object_type=object_type,
                    exclude_rid=bool(request.excludeRid),
                )
            )
        flattened = normalized_rows

        total = int(result.get("total") or 0)
        if missing_object_type_response is not None and total <= 0:
            return missing_object_type_response

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


@foundry_router.post(
    "/{objectType}/search",
    response_model=SearchObjectsResponseV2,
)
@trace_endpoint("oms.query.search_objects_v2_foundry")
async def search_objects_v2_foundry(
    ontology: str,
    payload: Dict[str, Any] = Body(...),
    objectType: str = ...,
    branch: str = Query(default="main"),
    es: ElasticsearchServiceDep = ...,
) -> SearchObjectsResponseV2 | JSONResponse:
    db_name = validate_db_name(ontology)
    return await _search_objects_v2_impl(
        payload=payload,
        db_name=db_name,
        object_type=objectType,
        branch=branch,
        es=es,
    )


@foundry_router.post(
    "/{objectType}/count",
    response_model=CountObjectsResponseV2,
)
@trace_endpoint("oms.query.count_objects_v2")
async def count_objects_v2_oms(
    ontology: str,
    objectType: str = ...,
    branch: str = Query(default="main"),
    sdk_package_rid: Optional[str] = Query(default=None, alias="sdkPackageRid"),
    sdk_version: Optional[str] = Query(default=None, alias="sdkVersion"),
    es: ElasticsearchServiceDep = ...,
) -> CountObjectsResponseV2 | JSONResponse:
    _ = sdk_package_rid, sdk_version
    try:
        db_name = validate_db_name(ontology)
        object_type = validate_class_id(objectType)
        branch = validate_branch_name(branch)
        missing = await _ensure_object_type_exists(db_name=db_name, branch=branch, object_type=object_type)
        if missing is not None:
            return missing

        query = {
            "bool": {
                "must": [
                    {"term": {"class_id": object_type}},
                ]
            }
        }
        index_name = get_instances_index_name(db_name, branch=branch)
        total = await es.count(index=index_name, query=query)
        return CountObjectsResponseV2(count=int(total))
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
        logger.error("Foundry object count failed (%s/%s): %s", ontology, objectType, exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="InternalServerError",
            parameters={"message": "Failed to count objects"},
        )


# ---------------------------------------------------------------------------
# Aggregate Objects Endpoint (ES-native)
# ---------------------------------------------------------------------------


@foundry_router.post("/{objectType}/aggregate", response_model=None)
@trace_endpoint("oms.query.aggregate_objects_v2")
async def aggregate_objects_v2_oms(
    ontology: str,
    payload: Dict[str, Any] = Body(...),
    objectType: str = ...,
    branch: str = Query(default="main"),
    es: ElasticsearchServiceDep = ...,
) -> Dict[str, Any] | JSONResponse:
    """
    Foundry Aggregate Objects API v2 — ES-native implementation.

    Performs aggregation entirely within Elasticsearch using search(size=0, aggs=...)
    instead of loading all rows into memory.
    """
    try:
        db_name = validate_db_name(ontology)
        object_type = validate_class_id(objectType)
        branch = validate_branch_name(branch)
        missing = await _ensure_object_type_exists(db_name=db_name, branch=branch, object_type=object_type)
        if missing is not None:
            return missing

        aggregation_clauses, group_by_clauses = _parse_aggregation_clauses(payload)

        # Build the filter query (same as search: class_id filter + optional where)
        where_clause = payload.get("where") or payload.get("filter")
        if where_clause is not None:
            if not isinstance(where_clause, dict):
                raise ValueError("where must be an object")
            object_query = _to_es_properties_query(where_clause)
        else:
            object_query = _to_es_properties_query(_default_match_all_where())

        query = {
            "bool": {
                "must": [
                    {"term": {"class_id": object_type}},
                    object_query,
                ]
            }
        }

        es_aggs = _build_es_aggregation(aggregation_clauses, group_by_clauses)
        index_name = get_instances_index_name(db_name, branch=branch)

        result = await es.search(
            index=index_name,
            query=query,
            size=0,  # No hits needed, only aggregations
            aggregations=es_aggs,
        )

        total_count = int(result.get("total") or 0)
        es_aggregations = result.get("aggregations", {})

        return _parse_es_aggregation_response(
            es_aggregations,
            aggregation_clauses,
            group_by_clauses,
            total_count=total_count,
            accuracy_request=str(payload.get("accuracy") or "").strip() or None,
            include_compute_usage=payload.get("includeComputeUsage"),
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
        logger.error("Foundry object aggregate failed (%s/%s): %s", db_name, objectType, exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="InternalServerError",
            parameters={"message": "Failed to execute object aggregation"},
        )
