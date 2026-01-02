from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

from shared.errors.error_types import ErrorCategory, ErrorCode

ENTERPRISE_SCHEMA_VERSION = "1.0"


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


class EnterpriseOwner(str, Enum):
    USER = "user"
    SYSTEM = "system"
    OPERATOR = "operator"


class EnterpriseSubsystem(str, Enum):
    BFF = "BFF"
    OMS = "OMS"
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
    retryable: bool
    action: EnterpriseAction
    owner: EnterpriseOwner
    legacy_code: Optional[str] = None
    legacy_category: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "schema": ENTERPRISE_SCHEMA_VERSION,
            "code": self.code,
            "domain": self.domain.value,
            "class": self.error_class.value,
            "subsystem": self.subsystem,
            "severity": self.severity.value,
            "title": self.title,
            "http_status": self.http_status,
            "retryable": self.retryable,
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
    if name in {"objectify-worker", "objectify_worker", "objectify"}:
        return EnterpriseSubsystem.OBJECTIFY
    if name in {"pipeline-scheduler", "pipeline_scheduler"}:
        return EnterpriseSubsystem.PIPELINE
    if name in {"pipeline-worker", "pipeline_worker", "pipeline"}:
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


_ERROR_CODE_SPECS: Dict[ErrorCode, EnterpriseErrorSpec] = {
    ErrorCode.REQUEST_VALIDATION_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-0001",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Request validation failed",
        severity=EnterpriseSeverity.ERROR,
        default_http_status=422,
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
    "REPLAY_REQUIRED": EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-PIP-STA-1002",
        domain=EnterpriseDomain.PIPELINE,
        error_class=EnterpriseClass.STATE,
        title="Pipeline replay required",
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
    resolved_status = _resolve_http_status(spec, status_code, prefer_status_code=prefer_status_code)
    retryable = _resolve_retryable(spec, retryable_hint=retryable_hint)
    action = _resolve_action(spec)
    owner = _resolve_owner(spec)
    legacy_code = external_code or (code.value if isinstance(code, ErrorCode) else None)
    legacy_category = category.value if isinstance(category, ErrorCategory) else None
    return EnterpriseError(
        code=spec.code_template.format(subsystem=subsystem),
        domain=spec.domain,
        error_class=spec.error_class,
        subsystem=subsystem,
        severity=spec.severity,
        title=spec.title,
        http_status=resolved_status,
        retryable=retryable,
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
        retryable=_resolve_retryable(spec, retryable_hint=False),
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
