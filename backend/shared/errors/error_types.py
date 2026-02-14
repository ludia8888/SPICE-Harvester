from enum import Enum
from typing import Any, Mapping
from fastapi import HTTPException


class ErrorCategory(str, Enum):
    INPUT = "input"
    AUTH = "auth"
    PERMISSION = "permission"
    RESOURCE = "resource"
    CONFLICT = "conflict"
    RATE_LIMIT = "rate_limit"
    UPSTREAM = "upstream"
    INTERNAL = "internal"


class ErrorCode(str, Enum):
    REQUEST_VALIDATION_FAILED = "REQUEST_VALIDATION_FAILED"
    INPUT_SANITIZATION_FAILED = "INPUT_SANITIZATION_FAILED"
    JSON_DECODE_ERROR = "JSON_DECODE_ERROR"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    AUTH_INVALID = "AUTH_INVALID"
    AUTH_EXPIRED = "AUTH_EXPIRED"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    RESOURCE_GONE = "RESOURCE_GONE"
    RESOURCE_ALREADY_EXISTS = "RESOURCE_ALREADY_EXISTS"
    CONFLICT = "CONFLICT"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    RATE_LIMITED = "RATE_LIMITED"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"
    UPSTREAM_UNAVAILABLE = "UPSTREAM_UNAVAILABLE"
    DB_ERROR = "DB_ERROR"
    DB_UNAVAILABLE = "DB_UNAVAILABLE"
    DB_TIMEOUT = "DB_TIMEOUT"
    DB_CONSTRAINT_VIOLATION = "DB_CONSTRAINT_VIOLATION"
    TERMINUS_CONFLICT = "TERMINUS_CONFLICT"
    TERMINUS_UNAVAILABLE = "TERMINUS_UNAVAILABLE"
    OMS_UNAVAILABLE = "OMS_UNAVAILABLE"
    HTTP_ERROR = "HTTP_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    FEATURE_NOT_IMPLEMENTED = "FEATURE_NOT_IMPLEMENTED"

    # ── Storage / lakeFS ──
    STORAGE_ERROR = "STORAGE_ERROR"
    STORAGE_UNAVAILABLE = "STORAGE_UNAVAILABLE"
    LAKEFS_ERROR = "LAKEFS_ERROR"
    LAKEFS_NOT_FOUND = "LAKEFS_NOT_FOUND"
    LAKEFS_CONFLICT = "LAKEFS_CONFLICT"
    LAKEFS_AUTH_ERROR = "LAKEFS_AUTH_ERROR"

    # ── Elasticsearch ──
    ES_ERROR = "ES_ERROR"
    ES_UNAVAILABLE = "ES_UNAVAILABLE"
    ES_INDEX_NOT_FOUND = "ES_INDEX_NOT_FOUND"

    # ── Lineage ──
    LINEAGE_UNAVAILABLE = "LINEAGE_UNAVAILABLE"
    LINEAGE_RECORD_FAILED = "LINEAGE_RECORD_FAILED"

    # ── Ontology ──
    ONTOLOGY_NOT_FOUND = "ONTOLOGY_NOT_FOUND"
    ONTOLOGY_DUPLICATE = "ONTOLOGY_DUPLICATE"
    ONTOLOGY_VALIDATION_FAILED = "ONTOLOGY_VALIDATION_FAILED"
    ONTOLOGY_RELATIONSHIP_ERROR = "ONTOLOGY_RELATIONSHIP_ERROR"
    ONTOLOGY_CIRCULAR_REFERENCE = "ONTOLOGY_CIRCULAR_REFERENCE"
    ONTOLOGY_ATOMIC_UPDATE_FAILED = "ONTOLOGY_ATOMIC_UPDATE_FAILED"
    ONTOLOGY_DEPLOY_FAILED = "ONTOLOGY_DEPLOY_FAILED"

    # ── Pipeline ──
    PIPELINE_NOT_FOUND = "PIPELINE_NOT_FOUND"
    PIPELINE_BUILD_FAILED = "PIPELINE_BUILD_FAILED"
    PIPELINE_VALIDATION_FAILED = "PIPELINE_VALIDATION_FAILED"

    # ── Action ──
    ACTION_TYPE_NOT_FOUND = "ACTION_TYPE_NOT_FOUND"
    ACTION_INPUT_INVALID = "ACTION_INPUT_INVALID"
    ACTION_TEMPLATE_ERROR = "ACTION_TEMPLATE_ERROR"
    ACTION_BASE_STATE_NOT_FOUND = "ACTION_BASE_STATE_NOT_FOUND"
    ACTION_CONFLICT_POLICY_FAILED = "ACTION_CONFLICT_POLICY_FAILED"

    # ── Objectify ──
    OBJECTIFY_JOB_FAILED = "OBJECTIFY_JOB_FAILED"
    OBJECTIFY_MAPPING_ERROR = "OBJECTIFY_MAPPING_ERROR"
    OBJECTIFY_CONTRACT_ERROR = "OBJECTIFY_CONTRACT_ERROR"

    # ── Kafka ──
    KAFKA_PRODUCE_FAILED = "KAFKA_PRODUCE_FAILED"
    KAFKA_CONSUME_FAILED = "KAFKA_CONSUME_FAILED"

    # ── Data Connector ──
    CONNECTOR_ERROR = "CONNECTOR_ERROR"

    # ── Merge / PR ──
    MERGE_CONFLICT = "MERGE_CONFLICT"

    # ── Configuration ──
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"


def classified_http_exception(
    status_code: int,
    detail: str | Mapping[str, Any],
    *,
    code: "ErrorCode | None" = None,
    category: "ErrorCategory | None" = None,
    external_code: "str | Enum | None" = None,
    extra: "Mapping[str, Any] | None" = None,
    headers: "Mapping[str, str] | None" = None,
) -> "HTTPException":
    """Create an HTTPException that carries enterprise error classification.

    The existing ``http_exception_handler`` in ``error_response.py`` already
    extracts ``code`` / ``category`` when ``exc.detail`` is a dict, so this
    helper ensures automatic enterprise envelope treatment without changing
    the global handler.
    """
    from fastapi import HTTPException as _HTTPException  # noqa: WPS433

    if isinstance(detail, Mapping):
        body: dict[str, Any] = dict(detail)
    else:
        body = {"message": detail}
    if "message" not in body or not isinstance(body.get("message"), str):
        body["message"] = "HTTP error"
    if code is not None:
        body["code"] = code.value
    if category is not None:
        body["category"] = category.value
    if external_code is not None:
        body["error_code"] = external_code.value if isinstance(external_code, Enum) else str(external_code)
    if extra:
        body.update(dict(extra))
    return _HTTPException(
        status_code=status_code,
        detail=body,
        headers=(dict(headers) if headers else None),
    )
