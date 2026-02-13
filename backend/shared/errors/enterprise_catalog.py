from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Tuple

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.config.settings import get_settings

ENTERPRISE_SCHEMA_VERSION = "1.1"
ENTERPRISE_CATALOG_REF = get_settings().observability.enterprise_catalog_ref_effective


class EnterpriseSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class EnterpriseDomain(str, Enum):
    INPUT = "input"
    ACCESS = "access"
    RESOURCE = "resource"
    CONFLICT = "conflict"
    RATE_LIMIT = "rate_limit"
    UPSTREAM = "upstream"
    DATABASE = "database"
    SYSTEM = "system"
    DATA = "data"
    MAPPING = "mapping"
    PIPELINE = "pipeline"
    ONTOLOGY = "ontology"
    OBJECTIFY = "objectify"


class EnterpriseClass(str, Enum):
    VALIDATION = "validation"
    SECURITY = "security"
    AUTH = "auth"
    PERMISSION = "permission"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    LIMIT = "limit"
    TIMEOUT = "timeout"
    UNAVAILABLE = "unavailable"
    INTERNAL = "internal"
    STATE = "state"
    INTEGRATION = "integration"


class EnterpriseAction(str, Enum):
    FIX_INPUT = "fix_input"
    REAUTH = "reauth"
    REQUEST_ACCESS = "request_access"
    CHECK_RESOURCE = "check_resource"
    RESOLVE_CONFLICT = "resolve_conflict"
    CHECK_STATE = "check_state"
    WAIT = "wait"
    RETRY = "retry"
    CHECK_UPSTREAM = "check_upstream"
    INVESTIGATE = "investigate"


class EnterpriseRetryPolicy(str, Enum):
    NONE = "none"
    BACKOFF = "backoff"
    IMMEDIATE = "immediate"
    AFTER_REFRESH = "after_refresh"


class EnterpriseJitterStrategy(str, Enum):
    NONE = "none"
    DETERMINISTIC_EQUAL_JITTER = "deterministic_equal_jitter"


class EnterpriseSafeNextAction(str, Enum):
    RETRY_BACKOFF = "retry_backoff"
    RETRY_IMMEDIATE = "retry_immediate"
    AFTER_REFRESH = "after_refresh"
    SAFE_MODE = "safe_mode"
    REQUEST_HUMAN = "request_human"


class EnterpriseOwner(str, Enum):
    USER = "user"
    SYSTEM = "system"
    OPERATOR = "operator"


class EnterpriseSubsystem(str, Enum):
    BFF = "BFF"
    OMS = "OMS"
    ACTION = "ACT"
    OBJECTIFY = "OBJ"
    PIPELINE = "PIP"
    PROJECTION = "PRJ"
    CONNECTOR = "CON"
    SHARED = "SHR"
    GENERIC = "GEN"


@dataclass(frozen=True)
class EnterpriseErrorSpec:
    code_template: str
    domain: EnterpriseDomain
    error_class: EnterpriseClass
    title: str
    severity: EnterpriseSeverity
    default_http_status: Optional[int] = None
    retryable: Optional[bool] = None
    default_retry_policy: Optional[EnterpriseRetryPolicy] = None
    max_attempts: Optional[int] = None
    base_delay_ms: Optional[int] = None
    max_delay_ms: Optional[int] = None
    jitter_strategy: Optional[EnterpriseJitterStrategy] = None
    retry_after_header_respect: Optional[bool] = None
    human_required: Optional[bool] = None
    runbook_ref: Optional[str] = None
    safe_next_actions: Optional[Tuple[EnterpriseSafeNextAction, ...]] = None
    action: Optional[EnterpriseAction] = None
    owner: Optional[EnterpriseOwner] = None


@dataclass(frozen=True)
class EnterpriseError:
    code: str
    domain: EnterpriseDomain
    error_class: EnterpriseClass
    subsystem: str
    severity: EnterpriseSeverity
    title: str
    http_status: int
    http_status_hint: int
    retryable: bool
    default_retry_policy: EnterpriseRetryPolicy
    max_attempts: int
    base_delay_ms: int
    max_delay_ms: int
    jitter_strategy: EnterpriseJitterStrategy
    retry_after_header_respect: bool
    human_required: bool
    runbook_ref: str
    safe_next_actions: Tuple[EnterpriseSafeNextAction, ...]
    action: EnterpriseAction
    owner: EnterpriseOwner
    legacy_code: Optional[str] = None
    legacy_category: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "schema": ENTERPRISE_SCHEMA_VERSION,
            "catalog_ref": ENTERPRISE_CATALOG_REF,
            "catalog_fingerprint": enterprise_catalog_fingerprint(),
            "code": self.code,
            "domain": self.domain.value,
            "class": self.error_class.value,
            "subsystem": self.subsystem,
            "severity": self.severity.value,
            "title": self.title,
            "http_status": self.http_status,
            "http_status_hint": self.http_status_hint,
            "retryable": self.retryable,
            "default_retry_policy": self.default_retry_policy.value,
            "max_attempts": int(self.max_attempts),
            "base_delay_ms": int(self.base_delay_ms),
            "max_delay_ms": int(self.max_delay_ms),
            "jitter_strategy": self.jitter_strategy.value,
            "retry_after_header_respect": bool(self.retry_after_header_respect),
            "human_required": self.human_required,
            "runbook_ref": self.runbook_ref,
            "safe_next_actions": [action.value for action in self.safe_next_actions],
            "action": self.action.value,
            "owner": self.owner.value,
        }
        if self.legacy_code is not None:
            payload["legacy_code"] = self.legacy_code
        if self.legacy_category is not None:
            payload["legacy_category"] = self.legacy_category
        return payload


def _normalize_subsystem(service_name: Optional[str]) -> EnterpriseSubsystem:
    if not service_name:
        return EnterpriseSubsystem.GENERIC
    name = service_name.strip().lower()
    if name in {"bff", "api-bff"}:
        return EnterpriseSubsystem.BFF
    if name in {"oms", "ontology"}:
        return EnterpriseSubsystem.OMS
    if name in {"action-worker", "action_worker", "actionworker", "action"}:
        return EnterpriseSubsystem.ACTION
    if name in {"objectify-worker", "objectify_worker", "objectify"}:
        return EnterpriseSubsystem.OBJECTIFY
    if name in {"pipeline-scheduler", "pipeline_scheduler"}:
        return EnterpriseSubsystem.PIPELINE
    if name in {"pipeline-worker", "pipeline_worker", "pipeline"}:
        return EnterpriseSubsystem.PIPELINE
    if "ingest-reconciler" in name or "ingest_reconciler" in name:
        return EnterpriseSubsystem.PIPELINE
    if "projection" in name:
        return EnterpriseSubsystem.PROJECTION
    if name.startswith("connector-") or name.startswith("connector_") or "connector" in name:
        return EnterpriseSubsystem.CONNECTOR
    if name in {"shared"}:
        return EnterpriseSubsystem.SHARED
    return EnterpriseSubsystem.GENERIC


_DEFAULT_HTTP_STATUS_BY_CLASS: Dict[EnterpriseClass, int] = {
    EnterpriseClass.VALIDATION: 400,
    EnterpriseClass.SECURITY: 400,
    EnterpriseClass.AUTH: 401,
    EnterpriseClass.PERMISSION: 403,
    EnterpriseClass.NOT_FOUND: 404,
    EnterpriseClass.CONFLICT: 409,
    EnterpriseClass.LIMIT: 429,
    EnterpriseClass.TIMEOUT: 504,
    EnterpriseClass.UNAVAILABLE: 503,
    EnterpriseClass.INTERNAL: 500,
    EnterpriseClass.STATE: 409,
    EnterpriseClass.INTEGRATION: 502,
}

_DEFAULT_RETRYABLE_BY_CLASS: Dict[EnterpriseClass, bool] = {
    EnterpriseClass.VALIDATION: False,
    EnterpriseClass.SECURITY: False,
    EnterpriseClass.AUTH: False,
    EnterpriseClass.PERMISSION: False,
    EnterpriseClass.NOT_FOUND: False,
    EnterpriseClass.CONFLICT: False,
    EnterpriseClass.STATE: False,
    EnterpriseClass.LIMIT: True,
    EnterpriseClass.TIMEOUT: True,
    EnterpriseClass.UNAVAILABLE: True,
    EnterpriseClass.INTEGRATION: False,
    EnterpriseClass.INTERNAL: False,
}

_DEFAULT_ACTION_BY_CLASS: Dict[EnterpriseClass, EnterpriseAction] = {
    EnterpriseClass.VALIDATION: EnterpriseAction.FIX_INPUT,
    EnterpriseClass.SECURITY: EnterpriseAction.FIX_INPUT,
    EnterpriseClass.AUTH: EnterpriseAction.REAUTH,
    EnterpriseClass.PERMISSION: EnterpriseAction.REQUEST_ACCESS,
    EnterpriseClass.NOT_FOUND: EnterpriseAction.CHECK_RESOURCE,
    EnterpriseClass.CONFLICT: EnterpriseAction.RESOLVE_CONFLICT,
    EnterpriseClass.STATE: EnterpriseAction.CHECK_STATE,
    EnterpriseClass.LIMIT: EnterpriseAction.WAIT,
    EnterpriseClass.TIMEOUT: EnterpriseAction.RETRY,
    EnterpriseClass.UNAVAILABLE: EnterpriseAction.RETRY,
    EnterpriseClass.INTEGRATION: EnterpriseAction.CHECK_UPSTREAM,
    EnterpriseClass.INTERNAL: EnterpriseAction.INVESTIGATE,
}

_DEFAULT_OWNER_BY_CLASS: Dict[EnterpriseClass, EnterpriseOwner] = {
    EnterpriseClass.VALIDATION: EnterpriseOwner.USER,
    EnterpriseClass.SECURITY: EnterpriseOwner.USER,
    EnterpriseClass.AUTH: EnterpriseOwner.USER,
    EnterpriseClass.PERMISSION: EnterpriseOwner.USER,
    EnterpriseClass.NOT_FOUND: EnterpriseOwner.USER,
    EnterpriseClass.CONFLICT: EnterpriseOwner.USER,
    EnterpriseClass.STATE: EnterpriseOwner.USER,
    EnterpriseClass.LIMIT: EnterpriseOwner.SYSTEM,
    EnterpriseClass.TIMEOUT: EnterpriseOwner.SYSTEM,
    EnterpriseClass.UNAVAILABLE: EnterpriseOwner.SYSTEM,
    EnterpriseClass.INTEGRATION: EnterpriseOwner.OPERATOR,
    EnterpriseClass.INTERNAL: EnterpriseOwner.OPERATOR,
}

_DEFAULT_RETRY_POLICY_BY_CLASS: Dict[EnterpriseClass, EnterpriseRetryPolicy] = {
    EnterpriseClass.TIMEOUT: EnterpriseRetryPolicy.BACKOFF,
    EnterpriseClass.UNAVAILABLE: EnterpriseRetryPolicy.BACKOFF,
    EnterpriseClass.LIMIT: EnterpriseRetryPolicy.BACKOFF,
    EnterpriseClass.AUTH: EnterpriseRetryPolicy.AFTER_REFRESH,
}

_DEFAULT_MAX_ATTEMPTS_BY_RETRY_POLICY: Dict[EnterpriseRetryPolicy, int] = {
    EnterpriseRetryPolicy.NONE: 1,
    EnterpriseRetryPolicy.IMMEDIATE: 2,
    EnterpriseRetryPolicy.BACKOFF: 3,
    EnterpriseRetryPolicy.AFTER_REFRESH: 2,
}

_DEFAULT_BASE_DELAY_MS_BY_RETRY_POLICY: Dict[EnterpriseRetryPolicy, int] = {
    EnterpriseRetryPolicy.NONE: 0,
    EnterpriseRetryPolicy.IMMEDIATE: 0,
    EnterpriseRetryPolicy.AFTER_REFRESH: 0,
    EnterpriseRetryPolicy.BACKOFF: 500,
}

_DEFAULT_MAX_DELAY_MS_BY_RETRY_POLICY: Dict[EnterpriseRetryPolicy, int] = {
    EnterpriseRetryPolicy.NONE: 0,
    EnterpriseRetryPolicy.IMMEDIATE: 0,
    EnterpriseRetryPolicy.AFTER_REFRESH: 0,
    EnterpriseRetryPolicy.BACKOFF: 10_000,
}

_DEFAULT_JITTER_STRATEGY_BY_RETRY_POLICY: Dict[EnterpriseRetryPolicy, EnterpriseJitterStrategy] = {
    EnterpriseRetryPolicy.NONE: EnterpriseJitterStrategy.NONE,
    EnterpriseRetryPolicy.IMMEDIATE: EnterpriseJitterStrategy.NONE,
    EnterpriseRetryPolicy.AFTER_REFRESH: EnterpriseJitterStrategy.NONE,
    EnterpriseRetryPolicy.BACKOFF: EnterpriseJitterStrategy.DETERMINISTIC_EQUAL_JITTER,
}

_DEFAULT_RETRY_AFTER_HEADER_RESPECT_BY_CLASS: Dict[EnterpriseClass, bool] = {}

_DEFAULT_HUMAN_REQUIRED_BY_CLASS: Dict[EnterpriseClass, bool] = {
    EnterpriseClass.TIMEOUT: False,
    EnterpriseClass.UNAVAILABLE: False,
    EnterpriseClass.LIMIT: False,
    EnterpriseClass.AUTH: False,
    EnterpriseClass.VALIDATION: True,
    EnterpriseClass.SECURITY: True,
    EnterpriseClass.PERMISSION: True,
    EnterpriseClass.NOT_FOUND: True,
    EnterpriseClass.CONFLICT: True,
    EnterpriseClass.STATE: True,
    EnterpriseClass.INTEGRATION: True,
    EnterpriseClass.INTERNAL: True,
}


_ERROR_CODE_SPECS: Dict[ErrorCode, EnterpriseErrorSpec] = {
    ErrorCode.REQUEST_VALIDATION_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-0001",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Request validation failed",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=400,
    ),
    ErrorCode.JSON_DECODE_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-0002",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Invalid JSON payload",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.INPUT_SANITIZATION_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-SEC-0001",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.SECURITY,
        title="Input rejected by security policy",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.PAYLOAD_TOO_LARGE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-LIM-0001",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.LIMIT,
        title="Payload too large",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=413,
        retryable=False,
        default_retry_policy=EnterpriseRetryPolicy.NONE,
        human_required=True,
        safe_next_actions=(EnterpriseSafeNextAction.REQUEST_HUMAN,),
        action=EnterpriseAction.FIX_INPUT,
        owner=EnterpriseOwner.USER,
    ),
    ErrorCode.AUTH_REQUIRED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-AUT-0001",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.AUTH,
        title="Authentication required",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.AUTH_INVALID: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-AUT-0002",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.AUTH,
        title="Invalid authentication",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.AUTH_EXPIRED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-AUT-0003",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.AUTH,
        title="Authentication expired",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.PERMISSION_DENIED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-PER-0001",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.PERMISSION,
        title="Permission denied",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.RESOURCE_NOT_FOUND: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-RES-NOT-0001",
        domain=EnterpriseDomain.RESOURCE,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Resource not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.RESOURCE_GONE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-RES-NOT-0002",
        domain=EnterpriseDomain.RESOURCE,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Resource is no longer available",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=410,
    ),
    ErrorCode.RESOURCE_ALREADY_EXISTS: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-RES-CON-0001",
        domain=EnterpriseDomain.RESOURCE,
        error_class=EnterpriseClass.CONFLICT,
        title="Resource already exists",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.CONFLICT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-CNF-CON-0001",
        domain=EnterpriseDomain.CONFLICT,
        error_class=EnterpriseClass.CONFLICT,
        title="Conflict detected",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.RATE_LIMITED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-RAT-LIM-0001",
        domain=EnterpriseDomain.RATE_LIMIT,
        error_class=EnterpriseClass.LIMIT,
        title="Rate limit exceeded",
        severity=EnterpriseSeverity.ERROR,
        retry_after_header_respect=True,
    ),
    ErrorCode.UPSTREAM_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-INTG-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTEGRATION,
        title="Upstream service error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.UPSTREAM_TIMEOUT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-TMO-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.TIMEOUT,
        title="Upstream request timed out",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.UPSTREAM_UNAVAILABLE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-UNA-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Upstream service unavailable",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.TERMINUS_CONFLICT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-CON-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.CONFLICT,
        title="Upstream conflict",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.TERMINUS_UNAVAILABLE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-UNA-0002",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Upstream service unavailable",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.OMS_UNAVAILABLE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-UNA-0003",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Upstream service unavailable",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.DB_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DB-INT-0001",
        domain=EnterpriseDomain.DATABASE,
        error_class=EnterpriseClass.INTERNAL,
        title="Database error",
        severity=EnterpriseSeverity.CRITICAL,
    ),
    ErrorCode.DB_UNAVAILABLE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DB-UNA-0001",
        domain=EnterpriseDomain.DATABASE,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Database unavailable",
        severity=EnterpriseSeverity.CRITICAL,
    ),
    ErrorCode.DB_TIMEOUT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DB-TMO-0001",
        domain=EnterpriseDomain.DATABASE,
        error_class=EnterpriseClass.TIMEOUT,
        title="Database timeout",
        severity=EnterpriseSeverity.CRITICAL,
    ),
    ErrorCode.DB_CONSTRAINT_VIOLATION: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DB-CON-0001",
        domain=EnterpriseDomain.DATABASE,
        error_class=EnterpriseClass.CONFLICT,
        title="Database constraint violation",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.HTTP_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-INT-0002",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.INTERNAL,
        title="HTTP error",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=400,
    ),
    ErrorCode.INTERNAL_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-INT-0001",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.INTERNAL,
        title="Internal server error",
        severity=EnterpriseSeverity.CRITICAL,
    ),
    ErrorCode.FEATURE_NOT_IMPLEMENTED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-STA-0001",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.STATE,
        title="Feature not implemented",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=501,
    ),
    # ── Storage / lakeFS ──
    ErrorCode.STORAGE_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-STO-INT-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTERNAL,
        title="Storage operation failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.STORAGE_UNAVAILABLE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-STO-UNA-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Storage service unavailable",
        severity=EnterpriseSeverity.CRITICAL,
    ),
    ErrorCode.LAKEFS_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-LFS-INT-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTEGRATION,
        title="lakeFS operation failed",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=503,
    ),
    ErrorCode.LAKEFS_NOT_FOUND: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-LFS-NOT-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.NOT_FOUND,
        title="lakeFS resource not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.LAKEFS_CONFLICT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-LFS-CON-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.CONFLICT,
        title="lakeFS merge conflict",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.LAKEFS_AUTH_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-LFS-AUT-0001",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.AUTH,
        title="lakeFS authentication failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    # ── Elasticsearch ──
    ErrorCode.ES_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ES-INT-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTERNAL,
        title="Elasticsearch operation failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ES_UNAVAILABLE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ES-UNA-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Elasticsearch unavailable",
        severity=EnterpriseSeverity.CRITICAL,
    ),
    ErrorCode.ES_INDEX_NOT_FOUND: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ES-NOT-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Elasticsearch index not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    # ── Ontology ──
    ErrorCode.ONTOLOGY_NOT_FOUND: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-NOT-0001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Ontology resource not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ONTOLOGY_DUPLICATE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-0001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Duplicate ontology resource",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ONTOLOGY_VALIDATION_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-0001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Ontology validation failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ONTOLOGY_RELATIONSHIP_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-0002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Ontology relationship error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ONTOLOGY_CIRCULAR_REFERENCE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-0003",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Circular reference detected in ontology",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ONTOLOGY_ATOMIC_UPDATE_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-0002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Ontology atomic update conflict",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ONTOLOGY_DEPLOY_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-STA-0001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.STATE,
        title="Ontology deployment failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    # ── Pipeline ──
    ErrorCode.PIPELINE_NOT_FOUND: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-NOT-0001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Pipeline not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.PIPELINE_BUILD_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-0001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Pipeline build failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.PIPELINE_VALIDATION_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-0001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Pipeline validation failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    # ── Action ──
    ErrorCode.ACTION_TYPE_NOT_FOUND: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACT-NOT-0001",
        domain=EnterpriseDomain.RESOURCE,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Action type not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ACTION_INPUT_INVALID: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACT-VAL-0001",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Action input validation failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ACTION_TEMPLATE_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACT-CON-0002",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.CONFLICT,
        title="Action template error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ACTION_BASE_STATE_NOT_FOUND: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACT-NOT-0002",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Action base instance state not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.ACTION_CONFLICT_POLICY_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACT-CON-0001",
        domain=EnterpriseDomain.CONFLICT,
        error_class=EnterpriseClass.CONFLICT,
        title="Action conflict policy rejected change",
        severity=EnterpriseSeverity.ERROR,
    ),
    # ── Objectify ──
    ErrorCode.OBJECTIFY_JOB_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-INT-0001",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.INTERNAL,
        title="Objectify job failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.OBJECTIFY_MAPPING_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-VAL-0001",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Objectify mapping error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.OBJECTIFY_CONTRACT_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-CON-0002",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.CONFLICT,
        title="Objectify contract error",
        severity=EnterpriseSeverity.ERROR,
    ),
    # ── Kafka ──
    ErrorCode.KAFKA_PRODUCE_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-KFK-INT-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTERNAL,
        title="Kafka produce failed",
        severity=EnterpriseSeverity.CRITICAL,
    ),
    ErrorCode.KAFKA_CONSUME_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-KFK-INT-0002",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTERNAL,
        title="Kafka consume failed",
        severity=EnterpriseSeverity.CRITICAL,
    ),
    ErrorCode.CONNECTOR_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-CON-INT-0001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTERNAL,
        title="Data connector error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCode.MERGE_CONFLICT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MRG-CON-0001",
        domain=EnterpriseDomain.CONFLICT,
        error_class=EnterpriseClass.CONFLICT,
        title="Merge conflict",
        severity=EnterpriseSeverity.WARNING,
    ),
    ErrorCode.CONFIGURATION_ERROR: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-CFG-INT-0001",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.INTERNAL,
        title="Configuration error",
        severity=EnterpriseSeverity.ERROR,
    ),
}

_CATEGORY_SPECS: Dict[ErrorCategory, EnterpriseErrorSpec] = {
    ErrorCategory.INPUT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-0999",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Input validation error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCategory.AUTH: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-AUT-0999",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.AUTH,
        title="Authentication error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCategory.PERMISSION: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-PER-0999",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.PERMISSION,
        title="Permission error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCategory.RESOURCE: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-RES-NOT-0999",
        domain=EnterpriseDomain.RESOURCE,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Resource error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCategory.CONFLICT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-CNF-CON-0999",
        domain=EnterpriseDomain.CONFLICT,
        error_class=EnterpriseClass.CONFLICT,
        title="Conflict error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCategory.RATE_LIMIT: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-RAT-LIM-0999",
        domain=EnterpriseDomain.RATE_LIMIT,
        error_class=EnterpriseClass.LIMIT,
        title="Rate limit error",
        severity=EnterpriseSeverity.ERROR,
        retry_after_header_respect=True,
    ),
    ErrorCategory.UPSTREAM: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-INTG-0999",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTEGRATION,
        title="Upstream error",
        severity=EnterpriseSeverity.ERROR,
    ),
    ErrorCategory.INTERNAL: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-INT-0999",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.INTERNAL,
        title="Internal error",
        severity=EnterpriseSeverity.CRITICAL,
    ),
}

_OBJECTIFY_ERROR_SPECS: Dict[str, EnterpriseErrorSpec] = {
    "dataset_not_found": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-NOT-0001",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Dataset not found",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=404,
    ),
    "dataset_db_name_mismatch": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-CON-0001",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.CONFLICT,
        title="Dataset database mismatch",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "dataset_version_mismatch": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-CON-0002",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.CONFLICT,
        title="Dataset version mismatch",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "objectify_input_conflict": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-CON-0001",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.CONFLICT,
        title="Objectify input conflict",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "objectify_input_missing": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-VAL-0001",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.VALIDATION,
        title="Objectify input missing",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=400,
    ),
    "artifact_key_missing": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-0001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Artifact key missing",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=400,
    ),
    "artifact_key_mismatch": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-0001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Artifact key mismatch",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "mapping_spec_not_found": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-NOT-0001",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Mapping spec not found",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=404,
    ),
    "mapping_spec_dataset_mismatch": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-CON-0001",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.CONFLICT,
        title="Mapping spec dataset mismatch",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "mapping_spec_version_mismatch": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-CON-0002",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.CONFLICT,
        title="Mapping spec version mismatch",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "artifact_not_found": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-NOT-0001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Artifact not found",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=404,
    ),
    "artifact_not_success": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-STA-0001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.STATE,
        title="Artifact not successful",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "artifact_not_build": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-STA-0002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.STATE,
        title="Artifact not a build output",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "artifact_outputs_missing": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-0002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Artifact outputs missing",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=400,
    ),
    "artifact_output_name_required": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-0003",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Artifact output name required",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=400,
    ),
    "artifact_output_name_missing": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-0003",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Artifact output name required",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=400,
    ),
    "artifact_output_not_found": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-NOT-0002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Artifact output not found",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=404,
    ),
    "artifact_output_ambiguous": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-0002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Artifact output ambiguous",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=409,
    ),
    "validation_failed": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-0001",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Data validation failed",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
    "no_rows_loaded": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-0002",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="No rows loaded",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
    "no_valid_instances": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-0003",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="No valid instances",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
}

_EXTERNAL_CODE_SPECS: Dict[str, EnterpriseErrorSpec] = {
    "MAPPING_SPEC_NOT_FOUND": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-NOT-1001",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Mapping spec not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-CON-1001",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.CONFLICT,
        title="Mapping spec mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_BACKING_SCHEMA_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-CON-3001",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.CONFLICT,
        title="Mapping spec backing schema mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_DATASET_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-CON-3002",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.CONFLICT,
        title="Mapping spec dataset mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_TARGET_UNKNOWN": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3001",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec target unknown",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_TARGET_TYPE_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3002",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec target type mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_SOURCE_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3003",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec source missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_REQUIRED_SOURCE_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3004",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec required source missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_REQUIRED_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3005",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec required targets missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_TITLE_KEY_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3006",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec title key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_UNIQUE_KEY_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3007",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec unique key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_PRIMARY_KEY_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3008",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec primary key mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_DATASET_PK_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3009",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec dataset primary key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_DATASET_PK_TARGET_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3010",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec dataset primary key target mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_SOURCE_TYPE_UNKNOWN": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3011",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec source type unknown",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_SOURCE_TYPE_UNSUPPORTED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3012",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec source type unsupported",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_RELATIONSHIP_REQUIRED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3013",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec relationship required",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3014",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec relationship cardinality unsupported",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_TYPE_INCOMPATIBLE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3015",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec type incompatible",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_UNSUPPORTED_TYPE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3016",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec unsupported type",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_UNSUPPORTED_TYPES": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3017",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec unsupported types",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MAPPING_SPEC_PRIMARY_KEY_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3018",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Mapping spec primary key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PIPELINE_ALREADY_EXISTS": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-1001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Pipeline already exists",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MERGE_NOT_SUPPORTED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-1001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Merge not supported",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MERGE_CONFLICT": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-1002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Merge conflict",
        severity=EnterpriseSeverity.ERROR,
    ),
    "BUILD_NOT_SUCCESS": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-STA-1001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.STATE,
        title="Build not successful",
        severity=EnterpriseSeverity.ERROR,
    ),
    "INVALID_BUILD_REPOSITORY": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-1002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Invalid build repository",
        severity=EnterpriseSeverity.ERROR,
    ),
    "INVALID_BUILD_REF": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-1003",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Invalid build reference",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PIPELINE_REQUEST_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-2001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Pipeline request invalid",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
    "PIPELINE_DEFINITION_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-2002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Pipeline definition invalid",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
    "PIPELINE_SCHEDULE_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-2003",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Pipeline schedule configuration invalid",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
    "PIPELINE_SCHEMA_CHECK_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-2001",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Pipeline schema checks failed",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
    "PIPELINE_SCHEMA_CONTRACT_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-2002",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Pipeline schema contract failed",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
    "PIPELINE_EXPECTATIONS_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-2003",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Pipeline expectations failed",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
    ),
    "PIPELINE_EXECUTION_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-INT-2001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.INTERNAL,
        title="Pipeline execution failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PIPELINE_PREFLIGHT_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-3001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Pipeline preflight failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "KEY_SPEC_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-3002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Key spec missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "KEY_SPEC_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-3003",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Key spec mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "KEY_SPEC_PRIMARY_KEY_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-VAL-3001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.VALIDATION,
        title="Key spec primary key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "KEY_SPEC_PRIMARY_KEY_TARGET_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-3004",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Key spec primary key target mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REPLAY_REQUIRED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-STA-1002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.STATE,
        title="Pipeline replay required",
        severity=EnterpriseSeverity.ERROR,
    ),
    "DEFINITION_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-1008",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Pipeline definition mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LAKEFS_MERGE_CONFLICT": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-1007",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="LakeFS merge conflict",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LAKEFS_MERGE_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-INTG-1001",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.INTEGRATION,
        title="LakeFS merge failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PROPOSAL_BUILD_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-1003",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Proposal build mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PROPOSAL_ARTIFACT_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-1004",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Proposal artifact mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PROPOSAL_DEFINITION_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-1005",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Proposal definition mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PROPOSAL_LAKEFS_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-CON-1006",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.CONFLICT,
        title="Proposal LakeFS mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PROPOSAL_ONTOLOGY_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-1001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Proposal ontology mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "ONTOLOGY_VERSION_UNKNOWN": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-NOT-1001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Ontology version unknown",
        severity=EnterpriseSeverity.ERROR,
    ),
    "ONTOLOGY_VERSION_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-1002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Ontology version mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "ONTOLOGY_GATE_UNAVAILABLE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-UNA-1001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Ontology gate unavailable",
        severity=EnterpriseSeverity.ERROR,
    ),
    "IFACE_NOT_FOUND": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-NOT-1002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Interface not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    "IFACE_MISSING_PROPERTY": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Interface missing property",
        severity=EnterpriseSeverity.ERROR,
    ),
    "IFACE_PROPERTY_TYPE_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Interface property type mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "IFACE_MISSING_RELATIONSHIP": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1003",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Interface missing relationship",
        severity=EnterpriseSeverity.ERROR,
    ),
    "IFACE_REL_TARGET_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1004",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Interface relationship target mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RESOURCE_SPEC_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1005",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Resource spec invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RESOURCE_OBJECT_TYPE_CONTRACT_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1006",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Object type contract missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RES001": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1007",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Resource required fields missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RES002": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1008",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Resource missing references",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RESOURCE_MISSING_REFERENCE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1008",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Resource missing references",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RESOURCE_UNUSED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1009",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Resource unused",
        severity=EnterpriseSeverity.WARNING,
    ),
    "MISSING_PREDICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1101",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship predicate missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_MISSING_PREDICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1101",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship predicate missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MISSING_TARGET": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1102",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship target missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_MISSING_TARGET": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1102",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship target missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "MISSING_LABEL": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1103",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship label missing",
        severity=EnterpriseSeverity.WARNING,
    ),
    "REL_EMPTY_LABEL": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1112",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship label empty",
        severity=EnterpriseSeverity.WARNING,
    ),
    "EMPTY_LABEL": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1112",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship label empty",
        severity=EnterpriseSeverity.WARNING,
    ),
    "INVALID_PREDICATE_FORMAT": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1104",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Predicate format invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_INVALID_PREDICATE_FORMAT": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1104",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Predicate format invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PREDICATE_NAMING_CONVENTION": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1105",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Predicate naming convention",
        severity=EnterpriseSeverity.INFO,
    ),
    "REL_PREDICATE_NAMING_CONVENTION": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1105",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Predicate naming convention",
        severity=EnterpriseSeverity.INFO,
    ),
    "INVALID_CARDINALITY": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1106",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Invalid cardinality",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_INVALID_CARDINALITY": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1106",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Invalid cardinality",
        severity=EnterpriseSeverity.ERROR,
    ),
    "CARDINALITY_RECOMMENDATION": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1107",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Cardinality recommendation",
        severity=EnterpriseSeverity.INFO,
    ),
    "REL_CARDINALITY_RECOMMENDATION": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1107",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Cardinality recommendation",
        severity=EnterpriseSeverity.INFO,
    ),
    "INVALID_TARGET_FORMAT": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1108",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Target class format invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_INVALID_TARGET_FORMAT": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1108",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Target class format invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "UNKNOWN_TARGET_CLASS": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1109",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Unknown target class",
        severity=EnterpriseSeverity.WARNING,
    ),
    "REL_UNKNOWN_TARGET_CLASS": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1109",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Unknown target class",
        severity=EnterpriseSeverity.WARNING,
    ),
    "SELF_REFERENCE_ONE_TO_ONE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1110",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Self reference one-to-one",
        severity=EnterpriseSeverity.WARNING,
    ),
    "REL_SELF_REFERENCE_ONE_TO_ONE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1110",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Self reference one-to-one",
        severity=EnterpriseSeverity.WARNING,
    ),
    "SELF_REFERENCE_DETECTED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1111",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Self reference detected",
        severity=EnterpriseSeverity.INFO,
    ),
    "REL_SELF_REFERENCE_DETECTED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1111",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Self reference detected",
        severity=EnterpriseSeverity.INFO,
    ),
    "INCOMPATIBLE_CARDINALITIES": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1113",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Incompatible cardinalities",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_INCOMPATIBLE_CARDINALITIES": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1113",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Incompatible cardinalities",
        severity=EnterpriseSeverity.ERROR,
    ),
    "UNUSUAL_CARDINALITY_PAIR": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1114",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Unusual cardinality pair",
        severity=EnterpriseSeverity.WARNING,
    ),
    "REL_UNUSUAL_CARDINALITY_PAIR": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1114",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Unusual cardinality pair",
        severity=EnterpriseSeverity.WARNING,
    ),
    "MISMATCHED_INVERSE_PREDICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1115",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Inverse predicate mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_MISMATCHED_INVERSE_PREDICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1115",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Inverse predicate mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "TARGET_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1116",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship target mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_TARGET_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1116",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship target mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "DUPLICATE_RELATIONSHIP": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1117",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Duplicate relationship",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_DUPLICATE_RELATIONSHIP": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1117",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Duplicate relationship",
        severity=EnterpriseSeverity.ERROR,
    ),
    "DUPLICATE_PREDICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1118",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Duplicate predicate",
        severity=EnterpriseSeverity.ERROR,
    ),
    "REL_DUPLICATE_PREDICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1118",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Duplicate predicate",
        severity=EnterpriseSeverity.ERROR,
    ),
    "ISOLATED_CLASS": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1119",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Isolated class",
        severity=EnterpriseSeverity.INFO,
    ),
    "REL_ISOLATED_CLASS": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1119",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Isolated class",
        severity=EnterpriseSeverity.INFO,
    ),
    "EXTERNAL_CLASS_REFERENCE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1120",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="External class reference",
        severity=EnterpriseSeverity.WARNING,
    ),
    "REL_EXTERNAL_CLASS_REFERENCE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1120",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="External class reference",
        severity=EnterpriseSeverity.WARNING,
    ),
    "GLOBAL_PREDICATE_CONFLICT": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1121",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Global predicate conflict",
        severity=EnterpriseSeverity.WARNING,
    ),
    "REL_GLOBAL_PREDICATE_CONFLICT": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-1121",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Global predicate conflict",
        severity=EnterpriseSeverity.WARNING,
    ),
    "ONTOLOGY_SCHEMA_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-NOT-3001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Ontology schema missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "VALUE_TYPE_NOT_FOUND": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-NOT-3002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Value type not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    "VALUE_TYPE_BASE_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Value type base mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "no_deployed_ontology": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-STA-3001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.STATE,
        title="No deployed ontology",
        severity=EnterpriseSeverity.ERROR,
    ),
    "action_type_not_found": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-NOT-3003",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Action type not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    "action_type_missing_writeback_target": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3001",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Action type writeback target missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "action_type_input_schema_invalid": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Action type input schema invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "action_type_implementation_invalid": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3003",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Action implementation invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "action_implementation_invalid": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3003",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Action implementation invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "action_implementation_compile_error": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3004",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Action implementation compile error",
        severity=EnterpriseSeverity.ERROR,
    ),
    "action_no_targets": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3005",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Action has no targets",
        severity=EnterpriseSeverity.ERROR,
    ),
    "validation_rules_invalid": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3006",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Validation rules invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "validation_rule_invalid": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3006",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Validation rules invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "submission_criteria_error": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3007",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Submission criteria invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "validation_rule_error": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3008",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Validation rule evaluation error",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_CONTRACT_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-NOT-3004",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Object type contract missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_INACTIVE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-STA-3002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.STATE,
        title="Object type inactive",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_EDIT_RESET_REQUIRED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-STA-3003",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.STATE,
        title="Object type edit reset required",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_MIGRATION_REQUIRED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-STA-3004",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.STATE,
        title="Object type migration required",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_PRIMARY_KEY_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3010",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Object type primary key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_TITLE_KEY_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3011",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Object type title key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_KEY_FIELDS_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3012",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Object type key fields missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_MAPPING_SPEC_REQUIRED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3013",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Object type mapping spec required",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_AUTO_MAPPING_EMPTY": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3014",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Object type auto mapping empty",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_BACKING_ARTIFACT_REQUIRED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3015",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Object type backing artifact required",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_BACKING_VERSION_REQUIRED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3016",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Object type backing version required",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_PRIMARY_KEY_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3002",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Object type primary key mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_SCHEMA_HASH_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3003",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Object type schema hash mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_BACKING_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3004",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Object type backing source mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_BACKING_DATASET_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3005",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Object type backing dataset mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LINK_EDITS_DISABLED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-STA-3005",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.STATE,
        title="Link edits disabled",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LINK_TYPE_PREDICATE_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3017",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Link type predicate missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_FK_TYPE_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3006",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Relationship FK type mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_SOURCE_PK_TYPE_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3007",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Relationship source PK type mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_JOIN_SOURCE_TYPE_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3008",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Relationship join source type mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_JOIN_TARGET_TYPE_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3009",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Relationship join target type mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LINK_EDIT_FETCH_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-INTG-3001",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.INTEGRATION,
        title="Link edit fetch failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LINK_EDIT_PREDICATE_UNKNOWN": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-VAL-3018",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.VALIDATION,
        title="Link edit predicate unknown",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LINK_EDIT_TARGET_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3010",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Link edit target invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LINK_EDIT_TARGET_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-NOT-3002",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Link edit target missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "LINK_EDIT_TYPE_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-3005",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Link edit type invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PRIMARY_KEY_MAPPING_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3020",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Primary key mapping invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_MAPPING_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-MAP-VAL-3021",
        domain=EnterpriseDomain.MAPPING,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship mapping missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_TARGET_LOOKUP_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-INTG-3001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTEGRATION,
        title="Relationship target lookup failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_TARGET_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-NOT-3001",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Relationship target missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_REF_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3007",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship reference invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_VALUE_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3008",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship value missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_VALUE_INVALID": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3009",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Relationship value invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_DUPLICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-CON-3004",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.CONFLICT,
        title="Duplicate relationship",
        severity=EnterpriseSeverity.ERROR,
    ),
    "RELATIONSHIP_CARDINALITY_VIOLATION": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-CON-3005",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.CONFLICT,
        title="Relationship cardinality violation",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PRIMARY_KEY_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3001",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Primary key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "PRIMARY_KEY_DUPLICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-CON-3001",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.CONFLICT,
        title="Primary key duplicate",
        severity=EnterpriseSeverity.ERROR,
    ),
    "ROW_KEY_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3002",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Row key missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "DUPLICATE_ROW_KEY": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-CON-3002",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.CONFLICT,
        title="Duplicate row key",
        severity=EnterpriseSeverity.ERROR,
    ),
    "UNIQUE_KEY_DUPLICATE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-CON-3003",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.CONFLICT,
        title="Unique key duplicate",
        severity=EnterpriseSeverity.ERROR,
    ),
    "VALUE_CONSTRAINT_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3003",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Value constraint failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "SOURCE_FIELD_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3004",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Source field missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "SOURCE_FIELD_UNKNOWN": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3005",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Source field unknown",
        severity=EnterpriseSeverity.ERROR,
    ),
    "TARGET_FIELD_UNKNOWN": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-VAL-3006",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.VALIDATION,
        title="Target field unknown",
        severity=EnterpriseSeverity.ERROR,
    ),
    "FULL_SYNC_FAILED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-INT-3001",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.INTERNAL,
        title="Full sync failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "NO_RELATIONSHIPS_INDEXED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-STA-3001",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.STATE,
        title="No relationships indexed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "action_input_invalid": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-3001",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Action input invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "simulation_assumption_invalid": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-3010",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Simulation assumptions invalid",
        severity=EnterpriseSeverity.ERROR,
    ),
    "simulation_assumption_forbidden_field": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-3011",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Simulation assumptions include forbidden fields",
        severity=EnterpriseSeverity.ERROR,
    ),
    "simulation_assumption_target_not_found": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-3012",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Simulation assumption target not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    "base_instance_not_found": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-NOT-3003",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Base instance not found",
        severity=EnterpriseSeverity.ERROR,
    ),
    "base_instance_state_unavailable": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-INT-3001",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.INTERNAL,
        title="Base instance state unavailable",
        severity=EnterpriseSeverity.ERROR,
    ),
    "submitted_by_required": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-3002",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="submitted_by required",
        severity=EnterpriseSeverity.ERROR,
    ),
    "submission_criteria_missing_user": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-3003",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Submission criteria missing user",
        severity=EnterpriseSeverity.ERROR,
    ),
    "validation_rule_failed": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-3004",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Validation rule failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "data_access_denied": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-PER-3001",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.PERMISSION,
        title="Data access denied",
        severity=EnterpriseSeverity.ERROR,
    ),
    "writeback_acl_denied": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-PER-3002",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.PERMISSION,
        title="Writeback ACL denied",
        severity=EnterpriseSeverity.ERROR,
    ),
    "writeback_acl_misaligned": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-PER-3003",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.PERMISSION,
        title="Writeback ACL misaligned",
        severity=EnterpriseSeverity.ERROR,
    ),
    "submission_criteria_failed": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-PER-3004",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.PERMISSION,
        title="Submission criteria failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "writeback_enforced": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-PER-3005",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.PERMISSION,
        title="Writeback enforced",
        severity=EnterpriseSeverity.ERROR,
    ),
    "writeback_acl_unverifiable": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-UNA-3001",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Writeback ACL unverifiable",
        severity=EnterpriseSeverity.ERROR,
    ),
    "writeback_governance_unavailable": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ACC-UNA-3002",
        domain=EnterpriseDomain.ACCESS,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Writeback governance unavailable",
        severity=EnterpriseSeverity.ERROR,
    ),
    "overlay_degraded": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-UNA-3001",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Overlay degraded",
        severity=EnterpriseSeverity.ERROR,
        retryable=False,
        default_retry_policy=EnterpriseRetryPolicy.NONE,
        human_required=True,
        runbook_ref="overlay_degraded",
        safe_next_actions=(EnterpriseSafeNextAction.SAFE_MODE, EnterpriseSafeNextAction.REQUEST_HUMAN),
    ),
    "context7_unavailable": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-UNA-3001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Context7 unavailable",
        severity=EnterpriseSeverity.ERROR,
    ),
    "conflict_detected": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-CNF-CON-3001",
        domain=EnterpriseDomain.CONFLICT,
        error_class=EnterpriseClass.CONFLICT,
        title="Conflict detected",
        severity=EnterpriseSeverity.ERROR,
    ),
    "unknown_label_keys": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-2001",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Unknown label keys",
        severity=EnterpriseSeverity.ERROR,
    ),
    "optimistic_concurrency_conflict": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-CNF-CON-2001",
        domain=EnterpriseDomain.CONFLICT,
        error_class=EnterpriseClass.CONFLICT,
        title="Optimistic concurrency conflict",
        severity=EnterpriseSeverity.ERROR,
    ),
    "rate_limiter_unavailable": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-RAT-UNA-2001",
        domain=EnterpriseDomain.RATE_LIMIT,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Rate limiter unavailable",
        severity=EnterpriseSeverity.ERROR,
    ),
    "timeout": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-TMO-2001",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.TIMEOUT,
        title="Operation timed out",
        severity=EnterpriseSeverity.ERROR,
    ),
    "reconciler_timeout": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-SYS-TMO-2002",
        domain=EnterpriseDomain.SYSTEM,
        error_class=EnterpriseClass.TIMEOUT,
        title="Reconciler timed out",
        severity=EnterpriseSeverity.ERROR,
    ),
    "SHEET_NOT_ACCESSIBLE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-UNA-2001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Google Sheet not accessible",
        severity=EnterpriseSeverity.ERROR,
    ),
    # LLM-related errors for agent autonomous loop
    "llm_output_invalid_json": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-VAL-4001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.VALIDATION,
        title="LLM output invalid JSON",
        severity=EnterpriseSeverity.WARNING,
    ),
    "llm_request_failed": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-INTG-4001",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.INTEGRATION,
        title="LLM request failed",
        severity=EnterpriseSeverity.ERROR,
    ),
    "FUNNEL_UNAVAILABLE": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-UPS-UNA-2101",
        domain=EnterpriseDomain.UPSTREAM,
        error_class=EnterpriseClass.UNAVAILABLE,
        title="Funnel service unavailable",
        severity=EnterpriseSeverity.ERROR,
        retryable=True,
        default_retry_policy=EnterpriseRetryPolicy.BACKOFF,
        safe_next_actions=(EnterpriseSafeNextAction.RETRY_BACKOFF,),
    ),
    "DATASET_VERSION_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-DAT-NOT-2101",
        domain=EnterpriseDomain.DATA,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Dataset version missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECT_TYPE_BACKING_DATASET_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-NOT-3101",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.NOT_FOUND,
        title="Object type backing dataset missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "ONTOLOGY_OBJECT_TYPE_PRIMARY_KEY_MISMATCH": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-ONT-CON-3101",
        domain=EnterpriseDomain.ONTOLOGY,
        error_class=EnterpriseClass.CONFLICT,
        title="Ontology/object type primary key mismatch",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECTIFY_DAG_CYCLE_DETECTED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-VAL-3101",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.VALIDATION,
        title="Objectify DAG cycle detected",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECTIFY_DAG_DEPENDENCIES_MISSING": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-VAL-3102",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.VALIDATION,
        title="Objectify DAG dependencies missing",
        severity=EnterpriseSeverity.ERROR,
    ),
    "OBJECTIFY_PRIMARY_KEY_OVERRIDE_IGNORED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-VAL-3103",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.VALIDATION,
        title="Objectify primary key override ignored",
        severity=EnterpriseSeverity.WARNING,
        default_http_status=200,
    ),
    "OBJECTIFY_PRIMARY_KEY_ORDER_OVERRIDE_IGNORED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-OBJ-VAL-3104",
        domain=EnterpriseDomain.OBJECTIFY,
        error_class=EnterpriseClass.VALIDATION,
        title="Objectify primary key order override ignored",
        severity=EnterpriseSeverity.WARNING,
        default_http_status=200,
    ),
}


def is_external_code(value: str) -> bool:
    if not value:
        return False
    return value in _EXTERNAL_CODE_SPECS


def _resolve_http_status(
    spec: EnterpriseErrorSpec,
    status_code: int,
    *,
    prefer_status_code: bool = False,
) -> int:
    if prefer_status_code:
        return status_code
    if spec.default_http_status is not None:
        return spec.default_http_status
    return _DEFAULT_HTTP_STATUS_BY_CLASS.get(spec.error_class, status_code)


def _resolve_retryable(
    spec: EnterpriseErrorSpec,
    *,
    retryable_hint: Optional[bool] = None,
) -> bool:
    if spec.retryable is not None:
        return spec.retryable
    if retryable_hint is not None:
        return retryable_hint
    return _DEFAULT_RETRYABLE_BY_CLASS.get(spec.error_class, False)


def _resolve_http_status_hint(spec: EnterpriseErrorSpec, status_code: int) -> int:
    if spec.default_http_status is not None:
        return spec.default_http_status
    return _DEFAULT_HTTP_STATUS_BY_CLASS.get(spec.error_class, status_code)


def _resolve_default_retry_policy(spec: EnterpriseErrorSpec) -> EnterpriseRetryPolicy:
    if spec.default_retry_policy is not None:
        return spec.default_retry_policy
    return _DEFAULT_RETRY_POLICY_BY_CLASS.get(spec.error_class, EnterpriseRetryPolicy.NONE)


def _resolve_human_required(spec: EnterpriseErrorSpec) -> bool:
    if spec.human_required is not None:
        return bool(spec.human_required)
    return _DEFAULT_HUMAN_REQUIRED_BY_CLASS.get(spec.error_class, True)


def _resolve_max_attempts(spec: EnterpriseErrorSpec, *, retry_policy: EnterpriseRetryPolicy) -> int:
    if spec.max_attempts is not None:
        try:
            return max(1, int(spec.max_attempts))
        except (TypeError, ValueError):
            return 1
    return _DEFAULT_MAX_ATTEMPTS_BY_RETRY_POLICY.get(retry_policy, 1)


def _resolve_base_delay_ms(spec: EnterpriseErrorSpec, *, retry_policy: EnterpriseRetryPolicy) -> int:
    if spec.base_delay_ms is not None:
        try:
            return max(0, int(spec.base_delay_ms))
        except (TypeError, ValueError):
            return 0
    return _DEFAULT_BASE_DELAY_MS_BY_RETRY_POLICY.get(retry_policy, 0)


def _resolve_max_delay_ms(spec: EnterpriseErrorSpec, *, retry_policy: EnterpriseRetryPolicy) -> int:
    if spec.max_delay_ms is not None:
        try:
            return max(0, int(spec.max_delay_ms))
        except (TypeError, ValueError):
            return 0
    return _DEFAULT_MAX_DELAY_MS_BY_RETRY_POLICY.get(retry_policy, 0)


def _resolve_jitter_strategy(spec: EnterpriseErrorSpec, *, retry_policy: EnterpriseRetryPolicy) -> EnterpriseJitterStrategy:
    if spec.jitter_strategy is not None:
        return spec.jitter_strategy
    return _DEFAULT_JITTER_STRATEGY_BY_RETRY_POLICY.get(retry_policy, EnterpriseJitterStrategy.NONE)


def _resolve_retry_after_header_respect(spec: EnterpriseErrorSpec) -> bool:
    if spec.retry_after_header_respect is not None:
        return bool(spec.retry_after_header_respect)
    return _DEFAULT_RETRY_AFTER_HEADER_RESPECT_BY_CLASS.get(spec.error_class, False)


_CATALOG_FINGERPRINT_CACHE: Optional[str] = None


def enterprise_catalog_fingerprint() -> str:
    global _CATALOG_FINGERPRINT_CACHE
    if _CATALOG_FINGERPRINT_CACHE:
        return _CATALOG_FINGERPRINT_CACHE

    def _spec_payload(spec: EnterpriseErrorSpec) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "code_template": spec.code_template,
            "domain": spec.domain.value,
            "class": spec.error_class.value,
            "title": spec.title,
            "severity": spec.severity.value,
            "default_http_status": spec.default_http_status,
            "retryable": spec.retryable,
            "default_retry_policy": spec.default_retry_policy.value if spec.default_retry_policy else None,
            "max_attempts": spec.max_attempts,
            "base_delay_ms": spec.base_delay_ms,
            "max_delay_ms": spec.max_delay_ms,
            "jitter_strategy": spec.jitter_strategy.value if spec.jitter_strategy else None,
            "retry_after_header_respect": spec.retry_after_header_respect,
            "human_required": spec.human_required,
            "runbook_ref": spec.runbook_ref,
            "safe_next_actions": (
                [action.value for action in spec.safe_next_actions] if spec.safe_next_actions else None
            ),
            "action": spec.action.value if spec.action else None,
            "owner": spec.owner.value if spec.owner else None,
        }
        return payload

    payload: Dict[str, object] = {
        "schema": ENTERPRISE_SCHEMA_VERSION,
        "error_codes": {k.value: _spec_payload(v) for k, v in sorted(_ERROR_CODE_SPECS.items(), key=lambda kv: kv[0].value)},  # type: ignore[name-defined]
        "categories": {k.value: _spec_payload(v) for k, v in sorted(_CATEGORY_SPECS.items(), key=lambda kv: kv[0].value)},  # type: ignore[name-defined]
        "objectify": {k: _spec_payload(v) for k, v in sorted(_OBJECTIFY_ERROR_SPECS.items(), key=lambda kv: kv[0])},  # type: ignore[name-defined]
        "external": {k: _spec_payload(v) for k, v in sorted(_EXTERNAL_CODE_SPECS.items(), key=lambda kv: kv[0])},  # type: ignore[name-defined]
    }
    _CATALOG_FINGERPRINT_CACHE = sha256_canonical_json_prefixed(payload)
    return _CATALOG_FINGERPRINT_CACHE

def _resolve_runbook_ref(spec: EnterpriseErrorSpec, *, legacy_code: Optional[str]) -> str:
    if spec.runbook_ref is not None and str(spec.runbook_ref).strip():
        return str(spec.runbook_ref).strip()
    return str(legacy_code or "unknown_error").strip() or "unknown_error"


def _resolve_safe_next_actions(
    spec: EnterpriseErrorSpec,
    *,
    legacy_code: Optional[str],
    retry_policy: EnterpriseRetryPolicy,
    human_required: bool,
) -> Tuple[EnterpriseSafeNextAction, ...]:
    if spec.safe_next_actions is not None:
        return spec.safe_next_actions

    actions: list[EnterpriseSafeNextAction] = []
    if retry_policy == EnterpriseRetryPolicy.BACKOFF:
        actions.append(EnterpriseSafeNextAction.RETRY_BACKOFF)
    elif retry_policy == EnterpriseRetryPolicy.IMMEDIATE:
        actions.append(EnterpriseSafeNextAction.RETRY_IMMEDIATE)
    elif retry_policy == EnterpriseRetryPolicy.AFTER_REFRESH:
        actions.append(EnterpriseSafeNextAction.AFTER_REFRESH)

    if human_required:
        actions.append(EnterpriseSafeNextAction.REQUEST_HUMAN)

    if legacy_code == "overlay_degraded" and EnterpriseSafeNextAction.SAFE_MODE not in actions:
        actions.insert(0, EnterpriseSafeNextAction.SAFE_MODE)

    if not actions:
        actions.append(EnterpriseSafeNextAction.REQUEST_HUMAN)
    return tuple(actions)


def _resolve_action(spec: EnterpriseErrorSpec) -> EnterpriseAction:
    return spec.action or _DEFAULT_ACTION_BY_CLASS.get(spec.error_class, EnterpriseAction.INVESTIGATE)


def _resolve_owner(spec: EnterpriseErrorSpec) -> EnterpriseOwner:
    return spec.owner or _DEFAULT_OWNER_BY_CLASS.get(spec.error_class, EnterpriseOwner.OPERATOR)


def resolve_enterprise_error(
    *,
    service_name: str,
    code: Optional[ErrorCode],
    category: Optional[ErrorCategory],
    status_code: int,
    external_code: Optional[str] = None,
    retryable_hint: Optional[bool] = None,
    prefer_status_code: bool = False,
) -> EnterpriseError:
    spec = _EXTERNAL_CODE_SPECS.get(external_code) if external_code else None
    if spec is None and code is not None:
        spec = _ERROR_CODE_SPECS.get(code)
    if spec is None and category is not None:
        spec = _CATEGORY_SPECS.get(category)
    if spec is None:
        spec = _CATEGORY_SPECS.get(ErrorCategory.INTERNAL)

    subsystem = _normalize_subsystem(service_name).value
    legacy_code = external_code or (code.value if isinstance(code, ErrorCode) else None)
    legacy_category = category.value if isinstance(category, ErrorCategory) else None

    resolved_status = _resolve_http_status(spec, status_code, prefer_status_code=prefer_status_code)
    http_status_hint = _resolve_http_status_hint(spec, status_code)
    retryable = _resolve_retryable(spec, retryable_hint=retryable_hint)
    retry_policy = _resolve_default_retry_policy(spec)
    max_attempts = _resolve_max_attempts(spec, retry_policy=retry_policy)
    base_delay_ms = _resolve_base_delay_ms(spec, retry_policy=retry_policy)
    max_delay_ms = _resolve_max_delay_ms(spec, retry_policy=retry_policy)
    jitter_strategy = _resolve_jitter_strategy(spec, retry_policy=retry_policy)
    retry_after_header_respect = _resolve_retry_after_header_respect(spec)
    human_required = _resolve_human_required(spec)
    runbook_ref = _resolve_runbook_ref(spec, legacy_code=legacy_code)
    safe_next_actions = _resolve_safe_next_actions(
        spec,
        legacy_code=legacy_code,
        retry_policy=retry_policy,
        human_required=human_required,
    )
    action = _resolve_action(spec)
    owner = _resolve_owner(spec)
    return EnterpriseError(
        code=spec.code_template.format(subsystem=subsystem),
        domain=spec.domain,
        error_class=spec.error_class,
        subsystem=subsystem,
        severity=spec.severity,
        title=spec.title,
        http_status=resolved_status,
        http_status_hint=http_status_hint,
        retryable=retryable,
        default_retry_policy=retry_policy,
        max_attempts=max_attempts,
        base_delay_ms=base_delay_ms,
        max_delay_ms=max_delay_ms,
        jitter_strategy=jitter_strategy,
        retry_after_header_respect=retry_after_header_respect,
        human_required=human_required,
        runbook_ref=runbook_ref,
        safe_next_actions=safe_next_actions,
        action=action,
        owner=owner,
        legacy_code=legacy_code,
        legacy_category=legacy_category,
    )


def resolve_objectify_error(error: str) -> Optional[EnterpriseError]:
    key = _normalize_objectify_error_key(error)
    if not key:
        return None
    spec = _OBJECTIFY_ERROR_SPECS.get(key)
    if spec is None:
        return None
    subsystem = EnterpriseSubsystem.OBJECTIFY.value
    resolved_status = _resolve_http_status(spec, 400)
    http_status_hint = _resolve_http_status_hint(spec, 400)
    retryable = _resolve_retryable(spec, retryable_hint=False)
    retry_policy = _resolve_default_retry_policy(spec)
    max_attempts = _resolve_max_attempts(spec, retry_policy=retry_policy)
    base_delay_ms = _resolve_base_delay_ms(spec, retry_policy=retry_policy)
    max_delay_ms = _resolve_max_delay_ms(spec, retry_policy=retry_policy)
    jitter_strategy = _resolve_jitter_strategy(spec, retry_policy=retry_policy)
    retry_after_header_respect = _resolve_retry_after_header_respect(spec)
    human_required = _resolve_human_required(spec)
    runbook_ref = _resolve_runbook_ref(spec, legacy_code=error)
    safe_next_actions = _resolve_safe_next_actions(
        spec,
        legacy_code=error,
        retry_policy=retry_policy,
        human_required=human_required,
    )
    action = _resolve_action(spec)
    owner = _resolve_owner(spec)
    return EnterpriseError(
        code=spec.code_template.format(subsystem=subsystem),
        domain=spec.domain,
        error_class=spec.error_class,
        subsystem=subsystem,
        severity=spec.severity,
        title=spec.title,
        http_status=resolved_status,
        http_status_hint=http_status_hint,
        retryable=retryable,
        default_retry_policy=retry_policy,
        max_attempts=max_attempts,
        base_delay_ms=base_delay_ms,
        max_delay_ms=max_delay_ms,
        jitter_strategy=jitter_strategy,
        retry_after_header_respect=retry_after_header_respect,
        human_required=human_required,
        runbook_ref=runbook_ref,
        safe_next_actions=safe_next_actions,
        action=action,
        owner=owner,
        legacy_code=error,
        legacy_category="objectify",
    )


def _normalize_objectify_error_key(error: str) -> str:
    if not error:
        return ""
    token = error.split(":", 1)[0]
    token = token.split("(", 1)[0]
    return token.strip()
