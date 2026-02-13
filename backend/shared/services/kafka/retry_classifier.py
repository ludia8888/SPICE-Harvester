"""Shared retry classification helpers for Kafka workers.

Keeps marker-based retry policy checks consistent across workers while letting
each worker define its own marker sets and type-based fast paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet, Iterable, Optional, Tuple

from shared.errors.error_types import ErrorCode


@dataclass(frozen=True)
class RetryPolicyProfile:
    non_retryable_markers: tuple[str, ...] = ()
    retryable_markers: tuple[str, ...] = ()
    default_retryable: bool = True


def normalize_error_message(exc: BaseException) -> str:
    return str(exc or "").lower()


def contains_marker(message: str, markers: Iterable[str]) -> bool:
    for marker in markers:
        token = str(marker or "").strip().lower()
        if token and token in message:
            return True
    return False


def classify_retryable_by_markers(
    exc: BaseException,
    *,
    non_retryable_markers: Iterable[str] = (),
    retryable_markers: Iterable[str] = (),
    default_retryable: bool = True,
) -> bool:
    message = normalize_error_message(exc)
    if contains_marker(message, non_retryable_markers):
        return False
    if contains_marker(message, retryable_markers):
        return True
    return bool(default_retryable)


def _normalize_markers(markers: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for marker in markers:
        token = str(marker or "").strip().lower()
        if token:
            normalized.append(token)
    return tuple(normalized)


def create_retry_policy_profile(
    *,
    non_retryable_markers: Iterable[str] = (),
    retryable_markers: Iterable[str] = (),
    default_retryable: bool = True,
) -> RetryPolicyProfile:
    return RetryPolicyProfile(
        non_retryable_markers=_normalize_markers(non_retryable_markers),
        retryable_markers=_normalize_markers(retryable_markers),
        default_retryable=bool(default_retryable),
    )


def classify_retryable_with_profile(exc: BaseException, profile: RetryPolicyProfile) -> bool:
    return classify_retryable_by_markers(
        exc,
        non_retryable_markers=profile.non_retryable_markers,
        retryable_markers=profile.retryable_markers,
        default_retryable=profile.default_retryable,
    )


ACTION_COMMAND_RETRY_PROFILE = create_retry_policy_profile(
    non_retryable_markers=(
        "permission denied",
        "validation",
        "schema",
        "bad request",
        "invalid",
        "missing",
    ),
    retryable_markers=(
        "timeout",
        "temporarily",
        "unavailable",
        "connection",
        "reset",
        "broken pipe",
        "429",
        "rate limit",
        "too many requests",
        "500",
        "502",
        "503",
        "504",
    ),
    default_retryable=True,
)

INSTANCE_COMMAND_RETRY_PROFILE = create_retry_policy_profile(
    non_retryable_markers=(
        "aggregate_id mismatch",
        "instance_id mismatch",
        "payload must be",
        "db_name is required",
        "class_id is required",
        "instance_id is required",
        "unknown command type",
        "security violation",
        "invalid",
        "bad request",
    ),
    default_retryable=True,
)

OBJECTIFY_JOB_RETRY_PROFILE = create_retry_policy_profile(
    non_retryable_markers=(
        "validation_failed",
        "mapping_spec_not_found",
        "mapping_spec_dataset_mismatch",
        "dataset_version_mismatch",
        "dataset_not_found",
        "db_name_mismatch",
        "artifact_key_mismatch",
        "invalid artifact_key",
        "invalid_artifact_key",
        "artifact_not_found",
        "artifact_not_success",
        "artifact_not_build",
        "artifact_outputs_missing",
        "artifact_output_not_found",
        "artifact_output_ambiguous",
        "artifact_output_name_required",
        "artifact_key_missing",
        "objectify_input_conflict",
        "objectify_input_missing",
        "no_rows_loaded",
    ),
    default_retryable=True,
)

ONTOLOGY_COMMAND_RETRY_PROFILE = create_retry_policy_profile(
    non_retryable_markers=(
        "schema check failure",
        "not_a_class_or_base_type",
        "security violation",
        "invalid",
        "bad request",
        "api error: 400",
    ),
    default_retryable=True,
)

SEARCH_PROJECTION_RETRY_PROFILE = create_retry_policy_profile(
    non_retryable_markers=(
        "mapper_parsing_exception",
        "document_parsing_exception",
        "illegal_argument_exception",
        "validation",
        "bad request",
    ),
    default_retryable=True,
)


# ── ErrorCode-based retry classification ──
# These sets provide O(1) lookup and are the preferred path for classifying
# retryability.  String-marker profiles above remain as a backward-compatible
# fallback for errors that do not carry a structured ErrorCode.

_NON_RETRYABLE_ERROR_CODES: FrozenSet[ErrorCode] = frozenset({
    # Input / validation
    ErrorCode.REQUEST_VALIDATION_FAILED,
    ErrorCode.INPUT_SANITIZATION_FAILED,
    ErrorCode.JSON_DECODE_ERROR,
    ErrorCode.PAYLOAD_TOO_LARGE,
    # Auth / permission
    ErrorCode.PERMISSION_DENIED,
    ErrorCode.AUTH_REQUIRED,
    ErrorCode.AUTH_INVALID,
    ErrorCode.AUTH_EXPIRED,
    ErrorCode.LAKEFS_AUTH_ERROR,
    # Resource
    ErrorCode.RESOURCE_NOT_FOUND,
    ErrorCode.RESOURCE_ALREADY_EXISTS,
    # Domain validation
    ErrorCode.ONTOLOGY_VALIDATION_FAILED,
    ErrorCode.ONTOLOGY_RELATIONSHIP_ERROR,
    ErrorCode.ONTOLOGY_CIRCULAR_REFERENCE,
    ErrorCode.PIPELINE_VALIDATION_FAILED,
    ErrorCode.ACTION_INPUT_INVALID,
    ErrorCode.ACTION_TEMPLATE_ERROR,
    ErrorCode.OBJECTIFY_MAPPING_ERROR,
    ErrorCode.OBJECTIFY_CONTRACT_ERROR,
    # Conflict (usually deterministic)
    ErrorCode.CONFLICT,
    ErrorCode.DB_CONSTRAINT_VIOLATION,
    ErrorCode.LAKEFS_CONFLICT,
    ErrorCode.ONTOLOGY_DUPLICATE,
    ErrorCode.ACTION_CONFLICT_POLICY_FAILED,
})

_RETRYABLE_ERROR_CODES: FrozenSet[ErrorCode] = frozenset({
    # Timeout / unavailable
    ErrorCode.UPSTREAM_TIMEOUT,
    ErrorCode.UPSTREAM_UNAVAILABLE,
    ErrorCode.DB_TIMEOUT,
    ErrorCode.DB_UNAVAILABLE,
    ErrorCode.ES_UNAVAILABLE,
    ErrorCode.STORAGE_UNAVAILABLE,
    ErrorCode.OMS_UNAVAILABLE,
    ErrorCode.TERMINUS_UNAVAILABLE,
    ErrorCode.RATE_LIMITED,
    # Transient infra
    ErrorCode.KAFKA_PRODUCE_FAILED,
    ErrorCode.KAFKA_CONSUME_FAILED,
})


def classify_retryable_by_error_code(code: Optional[ErrorCode]) -> Optional[bool]:
    """Classify retryability based on a structured ErrorCode.

    Returns ``True`` (retryable), ``False`` (non-retryable) or ``None``
    (unknown – caller should fall back to marker-based classification).
    """
    if code is None:
        return None
    if code in _NON_RETRYABLE_ERROR_CODES:
        return False
    if code in _RETRYABLE_ERROR_CODES:
        return True
    return None  # unknown – let marker-based fallback decide


__all__: Tuple[str, ...] = (
    "RetryPolicyProfile",
    "normalize_error_message",
    "contains_marker",
    "classify_retryable_by_markers",
    "create_retry_policy_profile",
    "classify_retryable_with_profile",
    "classify_retryable_by_error_code",
    "ACTION_COMMAND_RETRY_PROFILE",
    "INSTANCE_COMMAND_RETRY_PROFILE",
    "OBJECTIFY_JOB_RETRY_PROFILE",
    "ONTOLOGY_COMMAND_RETRY_PROFILE",
    "SEARCH_PROJECTION_RETRY_PROFILE",
)
