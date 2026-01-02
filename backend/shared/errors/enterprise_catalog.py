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


class EnterpriseSubsystem(str, Enum):
    BFF = "BFF"
    OMS = "OMS"
    OBJECTIFY = "OBJ"
    PIPELINE = "PIP"
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
    if name in {"objectify-worker", "objectify"}:
        return EnterpriseSubsystem.OBJECTIFY
    if name in {"pipeline-worker", "pipeline"}:
        return EnterpriseSubsystem.PIPELINE
    if name in {"shared"}:
        return EnterpriseSubsystem.SHARED
    return EnterpriseSubsystem.GENERIC


_ERROR_CODE_SPECS: Dict[ErrorCode, EnterpriseErrorSpec] = {
    ErrorCode.REQUEST_VALIDATION_FAILED: EnterpriseErrorSpec(
        code_template="SHV-{subsystem}-INP-VAL-0001",
        domain=EnterpriseDomain.INPUT,
        error_class=EnterpriseClass.VALIDATION,
        title="Request validation failed",
        severity=EnterpriseSeverity.ERROR,
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
        code_template="SHV-{subsystem}-UPS-INT-0001",
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
        code_template="SHV-{subsystem}-UPS-INT-0999",
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


def resolve_enterprise_error(
    *,
    service_name: str,
    code: Optional[ErrorCode],
    category: Optional[ErrorCategory],
    status_code: int,
    retryable: bool,
) -> EnterpriseError:
    spec = _ERROR_CODE_SPECS.get(code) if code is not None else None
    if spec is None and category is not None:
        spec = _CATEGORY_SPECS.get(category)
    if spec is None:
        spec = _CATEGORY_SPECS.get(ErrorCategory.INTERNAL)

    subsystem = _normalize_subsystem(service_name).value
    resolved_status = spec.default_http_status or status_code
    legacy_code = code.value if isinstance(code, ErrorCode) else None
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
    resolved_status = spec.default_http_status or 400
    return EnterpriseError(
        code=spec.code_template.format(subsystem=subsystem),
        domain=spec.domain,
        error_class=spec.error_class,
        subsystem=subsystem,
        severity=spec.severity,
        title=spec.title,
        http_status=resolved_status,
        retryable=False,
        legacy_code=error,
        legacy_category="objectify",
    )


def _normalize_objectify_error_key(error: str) -> str:
    if not error:
        return ""
    token = error.split(":", 1)[0]
    token = token.split("(", 1)[0]
    return token.strip()
