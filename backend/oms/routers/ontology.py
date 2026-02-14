"""
OMS 온톨로지 라우터 - 내부 ID 기반 온톨로지 관리
"""

import logging
import hmac
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request, status, Path, Query
from fastapi.responses import JSONResponse

# Modernized dependency injection imports
from oms.dependencies import (
    TerminusServiceDep,
    JSONLDConverterDep,
    LabelMapperDep,
    EventStoreDep,  # Added for S3/MinIO Event Store
    CommandStatusServiceDep,
    ValidatedDatabaseName,
    ValidatedClassId,
    ensure_database_exists
)
from oms.routers._event_sourcing import append_event_sourcing_command, build_command_status_metadata
from shared.models.commands import CommandType, OntologyCommand
from shared.config.app_config import AppConfig
from shared.utils.ontology_version import resolve_ontology_version
from shared.utils.language import coerce_localized_text, get_accept_language, select_localized_text
from shared.utils.ontology_type_normalization import normalize_ontology_base_type
from shared.services.core.ontology_linter import (
    OntologyLinterConfig,
    compute_risk_score,
    lint_ontology_create,
    lint_ontology_update,
    risk_level,
)
from shared.models.ontology_lint import LintReport

# OMS 서비스 import
from oms.services.async_terminus import AsyncTerminusService
from oms.services.ontology_interface_contract import (
    collect_interface_contract_issues,
    extract_interface_refs,
    strip_interface_prefix,
)
from oms.services.ontology_resources import OntologyResourceService
from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
from oms.validation_codes import OntologyValidationCode as OVC
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.models.common import BaseResponse
from shared.models.requests import ApiResponse
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception

# shared 모델 import
from shared.models.ontology import (
    OntologyCreateRequest,
    OntologyResponse,
    OntologyUpdateRequest,
    Property,
    QueryRequestInternal,
    QueryResponse,
)

# Add shared security module to path
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_branch_name,
    validate_class_id,
    validate_db_name,
)
from shared.observability.tracing import trace_endpoint


# Rate limiting import
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets
from shared.config.settings import get_settings
from shared.security.auth_utils import extract_presented_token, get_expected_token
from shared.utils.branch_utils import get_protected_branches, protected_branch_write_message

logger = logging.getLogger(__name__)
_PROPERTY_CONVERTER = PropertyToRelationshipConverter()


def _is_protected_branch(branch: str) -> bool:
    return branch in get_protected_branches()


def _require_proposal_for_branch(branch: str) -> bool:
    if not _is_protected_branch(branch):
        return False
    return bool(get_settings().ontology.require_proposals)


def _reject_direct_write_if_required(branch: str) -> None:
    if _require_proposal_for_branch(branch):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            protected_branch_write_message(),
            code=ErrorCode.CONFLICT,
        )


_ADMIN_TOKEN_ENV_KEYS = ("OMS_ADMIN_TOKEN", "BFF_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")


def _admin_authorized(request: Request) -> bool:
    expected = get_expected_token(_ADMIN_TOKEN_ENV_KEYS)
    if not expected:
        return False
    presented = extract_presented_token(request.headers)
    if not presented:
        return False
    return hmac.compare_digest(presented, expected)


def _extract_change_reason(request: Request) -> Optional[str]:
    reason = (request.headers.get("X-Change-Reason") or "").strip()
    return reason or None


def _extract_actor(request: Request) -> Optional[str]:
    actor = (request.headers.get("X-Admin-Actor") or request.headers.get("X-Actor") or "").strip()
    return actor or None


async def _collect_interface_issues(
    *,
    terminus: AsyncTerminusService,
    db_name: str,
    branch: str,
    ontology_id: str,
    metadata: Dict[str, Any],
    properties: List[Any],
    relationships: List[Any],
    resource_service: Optional[OntologyResourceService] = None,
) -> List[Dict[str, Any]]:
    refs = extract_interface_refs(metadata)
    if not refs:
        return []

    service = resource_service or OntologyResourceService(terminus)
    interface_index: Dict[str, Dict[str, Any]] = {}
    for raw_ref in refs:
        interface_id = strip_interface_prefix(raw_ref)
        if not interface_id:
            continue
        resource = await service.get_resource(
            db_name,
            branch=branch,
            resource_type="interface",
            resource_id=interface_id,
        )
        if resource:
            interface_index[interface_id] = resource

    return collect_interface_contract_issues(
        ontology_id=ontology_id,
        metadata=metadata,
        properties=properties,
        relationships=relationships,
        interface_index=interface_index,
    )


def _extract_shared_property_refs(metadata: Dict[str, Any]) -> List[str]:
    if not metadata:
        return []
    refs: List[str] = []
    for key in (
        "shared_properties",
        "shared_property_refs",
        "sharedPropertyRefs",
        "sharedPropertyRef",
    ):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            refs.append(value.strip())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    refs.append(item.strip())
    seen = set()
    ordered = []
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            ordered.append(ref)
    return ordered


def _extract_group_refs(metadata: Dict[str, Any]) -> List[str]:
    if not metadata:
        return []
    refs: List[str] = []
    for key in (
        "groups",
        "group_refs",
        "groupRefs",
        "groupRef",
        "group",
    ):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            refs.append(value.strip())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    refs.append(item.strip())
    seen = set()
    ordered = []
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            ordered.append(ref)
    return ordered


async def _validate_group_refs(
    *,
    terminus: AsyncTerminusService,
    db_name: str,
    branch: str,
    metadata: Dict[str, Any],
    resource_service: Optional[OntologyResourceService] = None,
) -> List[str]:
    refs = _extract_group_refs(metadata)
    if not refs:
        return []
    service = resource_service or OntologyResourceService(terminus)
    missing: List[str] = []
    for ref in refs:
        resource = await service.get_resource(
            db_name,
            branch=branch,
            resource_type="group",
            resource_id=ref,
        )
        if not resource:
            missing.append(ref)
    return sorted(set(missing))


async def _apply_shared_properties(
    *,
    terminus: AsyncTerminusService,
    db_name: str,
    branch: str,
    properties: List[Any],
    metadata: Dict[str, Any],
    resource_service: Optional[OntologyResourceService] = None,
) -> Tuple[List[Any], Dict[str, Any]]:
    refs = _extract_shared_property_refs(metadata)
    if not refs:
        return properties, {}

    service = resource_service or OntologyResourceService(terminus)
    merged = list(properties or [])
    existing_names = {getattr(p, "name", None) for p in merged}
    existing_names = {name for name in existing_names if name}

    missing: List[str] = []
    duplicate_names: List[str] = []
    invalid_defs: List[str] = []

    for ref in refs:
        resource = await service.get_resource(
            db_name,
            branch=branch,
            resource_type="shared_property",
            resource_id=ref,
        )
        if not resource:
            missing.append(ref)
            continue
        spec = resource.get("spec") if isinstance(resource, dict) else None
        props = spec.get("properties") if isinstance(spec, dict) else None
        if not isinstance(props, list) or not props:
            invalid_defs.append(ref)
            continue
        for prop_def in props:
            if not isinstance(prop_def, dict):
                invalid_defs.append(ref)
                continue
            name = str(prop_def.get("name") or "").strip()
            if not name:
                invalid_defs.append(ref)
                continue
            if name in existing_names:
                duplicate_names.append(name)
                continue
            payload = dict(prop_def)
            payload.setdefault("label", name)
            payload["shared_property_ref"] = ref
            try:
                merged.append(Property(**payload))
                existing_names.add(name)
            except Exception:
                logging.getLogger(__name__).warning("Broad exception fallback at oms/routers/ontology.py:306", exc_info=True)
                invalid_defs.append(ref)

    issues = {}
    if missing:
        issues["missing_shared_properties"] = sorted(set(missing))
    if duplicate_names:
        issues["duplicate_property_names"] = sorted(set(duplicate_names))
    if invalid_defs:
        issues["invalid_shared_properties"] = sorted(set(invalid_defs))
    return merged, issues


async def _validate_value_type_refs(
    *,
    terminus: AsyncTerminusService,
    db_name: str,
    branch: str,
    properties: List[Any],
    resource_service: Optional[OntologyResourceService] = None,
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    service = resource_service or OntologyResourceService(terminus)

    for prop in properties or []:
        value_type_ref = getattr(prop, "value_type_ref", None) or getattr(prop, "valueTypeRef", None)
        if not value_type_ref:
            continue
        value_type_ref = str(value_type_ref).strip()
        if not value_type_ref:
            continue
        resource = await service.get_resource(
            db_name,
            branch=branch,
            resource_type="value_type",
            resource_id=value_type_ref,
        )
        if not resource:
            issues.append(
                {
                    "code": OVC.VALUE_TYPE_NOT_FOUND.value,
                    "field": prop.name,
                    "value_type_ref": value_type_ref,
                    "message": f"Value type '{value_type_ref}' not found",
                }
            )
            continue
        spec = resource.get("spec") if isinstance(resource, dict) else None
        spec = spec if isinstance(spec, dict) else {}
        base_type = spec.get("base_type") or spec.get("baseType")
        if base_type:
            prop_type = getattr(prop, "type", None)
            if normalize_ontology_base_type(prop_type) != normalize_ontology_base_type(base_type):
                issues.append(
                    {
                        "code": OVC.VALUE_TYPE_BASE_MISMATCH.value,
                        "field": prop.name,
                        "value_type_ref": value_type_ref,
                        "expected_base_type": base_type,
                        "actual_type": prop_type,
                        "message": (
                            f"Property '{prop.name}' type '{prop_type}' does not match "
                            f"value type base '{base_type}'"
                        ),
                    }
                )

    return issues


def _is_internal_ontology(ontology: OntologyResponse) -> bool:
    if not ontology:
        return False
    metadata = ontology.metadata if isinstance(ontology.metadata, dict) else {}
    if metadata.get("internal"):
        return True
    return str(ontology.id or "").startswith("__")


def _localized_to_string(value: Any, *, lang: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        preferred = str(value.get(lang) or "").strip() if lang else ""
        if preferred:
            return preferred
        for fallback in ("en", "ko"):
            candidate = str(value.get(fallback) or "").strip()
            if candidate:
                return candidate
        for v in value.values():
            candidate = str(v).strip() if v is not None else ""
            if candidate:
                return candidate
        return None
    candidate = str(value).strip()
    return candidate or None


def _merge_lint_reports(*reports: LintReport) -> LintReport:
    errors = []
    warnings = []
    infos = []
    for report in reports:
        if not report:
            continue
        errors.extend(report.errors or [])
        warnings.extend(report.warnings or [])
        infos.extend(report.infos or [])

    score = compute_risk_score(errors, warnings, infos)
    return LintReport(
        ok=len(errors) == 0,
        risk_score=score,
        risk_level=risk_level(score),
        errors=errors,
        warnings=warnings,
        infos=infos,
    )


def _relationship_validation_enabled(flag: Optional[bool] = None) -> bool:
    env_enabled = bool(get_settings().ontology.validate_relationships)
    if flag is None:
        return env_enabled
    return env_enabled and bool(flag)


async def _validate_relationships_gate(
    *,
    terminus: AsyncTerminusService,
    db_name: str,
    branch: str,
    ontology_payload: Dict[str, Any],
    enabled: bool,
) -> Optional[JSONResponse]:
    if not enabled:
        return None
    validation_result = await terminus.validate_relationships(db_name, ontology_payload, branch=branch)
    if not validation_result.get("ok", False):
        error_payload = build_error_envelope(
            service_name="oms",
            message="온톨로지 관계 검증에 실패했습니다",
            detail="Relationship validation failed",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            errors=[issue.get("message") for issue in validation_result.get("errors") or []],
            context={"relationship_validation": validation_result},
        )
        return JSONResponse(status_code=error_payload["http_status"], content=error_payload)
    return None


async def _ensure_database_exists(db_name: str, terminus: AsyncTerminusService):
    """데이터베이스 존재 여부 확인 후 404 예외 발생"""
    # 데이터베이스 이름 보안 검증
    validated_db_name = validate_db_name(db_name)

    exists = await terminus.database_exists(validated_db_name)
    if not exists:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            f"데이터베이스 '{validated_db_name}'을(를) 찾을 수 없습니다",
            code=ErrorCode.RESOURCE_NOT_FOUND,
        )


router = APIRouter(prefix="/database/{db_name}/ontology", tags=["Ontology Management"])


@router.post(
    "",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_400_BAD_REQUEST: {"model": ApiResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Database not found"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
@rate_limit(**RateLimitPresets.WRITE)
@trace_endpoint("oms.ontology.create")
async def create_ontology(
    ontology_request: OntologyCreateRequest,  # Request body first (no default)
    request: Request,
    db_name: str = Path(..., description="Database name"),  # URL path parameter
    branch: str = Query("main", description="Target branch (default: main)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
    event_store=EventStoreDep,
    command_status_service=CommandStatusServiceDep,
) -> ApiResponse:
    """내부 ID 기반 온톨로지 생성"""
    try:
        enable_event_sourcing = bool(get_settings().event_sourcing.enable_event_sourcing)
        branch = validate_branch_name(branch)
        _reject_direct_write_if_required(branch)
        lang = get_accept_language(request)

        # 데이터베이스 존재 확인 (dependency 제거로 인해 수동 처리)
        db_name = validate_db_name(db_name)
        if not await terminus.database_exists(db_name):
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )
        
        # 요청 데이터를 dict로 변환
        ontology_data = ontology_request.model_dump()

        # 클래스 ID 검증
        class_id = ontology_data.get("id")
        if class_id:
            ontology_data["id"] = validate_class_id(class_id)

        # 기본 데이터 타입 검증
        if not ontology_data.get("id"):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "Ontology ID is required",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        raw_label = ontology_data.get("label", ontology_data.get("rdfs:label", ontology_data.get("id")))
        raw_description = ontology_data.get("description", ontology_data.get("rdfs:comment"))

        label_i18n = coerce_localized_text(raw_label)
        description_i18n = coerce_localized_text(raw_description) if raw_description is not None else {}

        label_display = select_localized_text(label_i18n, lang=lang) or str(ontology_data.get("id") or "Unknown")
        (
            select_localized_text(description_i18n, lang=lang) if description_i18n else None
        )

        metadata_payload = (
            ontology_data.get("metadata")
            if isinstance(ontology_data.get("metadata"), dict)
            else {}
        )
        expanded_properties, shared_prop_issues = await _apply_shared_properties(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=list(ontology_request.properties or []),
            metadata=metadata_payload,
        )
        if shared_prop_issues:
            error_payload = build_error_envelope(
                service_name="oms",
                message="공유 속성(shared property) 적용에 실패했습니다",
                detail="Shared property expansion failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=["Shared property expansion failed"],
                context={"shared_property_issues": shared_prop_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        value_type_issues = await _validate_value_type_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=expanded_properties,
        )
        if value_type_issues:
            issue_messages = [issue.get("message") for issue in value_type_issues]
            error_payload = build_error_envelope(
                service_name="oms",
                message="값 타입(value type) 검증에 실패했습니다",
                detail="Value type validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=issue_messages,
                context={"value_type_issues": value_type_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        missing_groups = await _validate_group_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            metadata=metadata_payload,
        )
        if missing_groups:
            error_payload = build_error_envelope(
                service_name="oms",
                message="그룹(group) 참조를 찾을 수 없습니다",
                detail="Group references not found",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=["Group references not found"],
                context={"missing_groups": missing_groups},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        missing_groups = await _validate_group_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            metadata=metadata_payload,
        )
        if missing_groups:
            error_payload = build_error_envelope(
                service_name="oms",
                message="그룹(group) 참조를 찾을 수 없습니다",
                detail="Group references not found",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=["Group references not found"],
                context={"missing_groups": missing_groups},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        lint_report = lint_ontology_create(
            class_id=str(ontology_data.get("id")),
            label=label_display,
            abstract=bool(ontology_data.get("abstract", False)),
            properties=expanded_properties,
            relationships=list(ontology_request.relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        if not lint_report.ok:
            lint_errors = [issue.message for issue in lint_report.errors]
            error_payload = build_error_envelope(
                service_name="oms",
                message="온톨로지 스키마 검증에 실패했습니다",
                detail="Ontology schema validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=lint_errors,
                context={"lint_report": lint_report.model_dump()},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=str(ontology_data.get("id") or ""),
            metadata=metadata_payload,
            properties=expanded_properties,
            relationships=list(ontology_request.relationships or []),
        )
        if interface_issues:
            issue_messages = [issue.get("message") for issue in interface_issues]
            error_payload = build_error_envelope(
                service_name="oms",
                message="온톨로지 인터페이스 계약 검증에 실패했습니다",
                detail="Ontology interface contract validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=issue_messages,
                context={"interface_issues": interface_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        ontology_data["properties"] = [
            p.model_dump() if hasattr(p, "model_dump") else p for p in expanded_properties
        ]

        try:
            ontology_data = _PROPERTY_CONVERTER.process_class_data(ontology_data)
        except Exception as e:
            logger.error("Property→relationship conversion failed: %s", e)
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"Failed to convert properties to relationships: {e}",
                code=ErrorCode.ONTOLOGY_RELATIONSHIP_ERROR,
            )

        relationship_payload = {
            "id": ontology_data.get("id"),
            "label": label_display or ontology_data.get("id"),
            "properties": ontology_data.get("properties") or [],
            "relationships": ontology_data.get("relationships") or [],
        }
        relationship_response = await _validate_relationships_gate(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_payload=relationship_payload,
            enabled=_relationship_validation_enabled(),
        )
        if relationship_response:
            return relationship_response

        if enable_event_sourcing:
            ontology_version = await resolve_ontology_version(
                terminus, db_name=db_name, branch=branch, logger=logger
            )
            # Event Sourcing: append command-request event to S3/MinIO and return 202.
            command = OntologyCommand(
                command_type=CommandType.CREATE_ONTOLOGY_CLASS,
                aggregate_id=f"{db_name}:{branch}:{ontology_data.get('id')}",
                db_name=db_name,
                branch=branch,
                expected_seq=0,
                payload={
                    "db_name": db_name,
                    "branch": branch,
                    "class_id": ontology_data.get("id"),
                    "label": label_i18n,
                    "description": description_i18n or None,
                    "properties": ontology_data.get("properties", []),
                    "relationships": ontology_data.get("relationships", []),
                    "parent_class": ontology_data.get("parent_class"),
                    "abstract": ontology_data.get("abstract", False),
                    "metadata": metadata_payload,
                },
                metadata={"source": "OMS", "user": "system", "ontology": ontology_version},
                created_by=_extract_actor(request),
            )

            envelope = await append_event_sourcing_command(
                event_store=event_store,
                command=command,
                actor=_extract_actor(request),
                kafka_topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC,
                command_status_service=command_status_service,
                command_status_metadata=build_command_status_metadata(
                    command=command,
                    extra={
                        "db_name": db_name,
                        "branch": branch,
                        "class_id": ontology_data.get("id"),
                    },
                ),
            )

            logger.info(
                f"🔥 Stored CREATE_ONTOLOGY_CLASS command in Event Store: {envelope.event_id} "
                f"(seq={envelope.sequence_number})"
            )

            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content=ApiResponse.accepted(
                    message=f"온톨로지 '{ontology_data.get('id')}' 생성 명령이 접수되었습니다",
                    data={
                        "command_id": str(command.command_id),
                        "ontology_id": ontology_data.get("id"),
                        "database": db_name,
                        "branch": branch,
                        "status": "processing",
                        "mode": "event_sourcing",
                    },
                ).to_dict(),
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "ENABLE_EVENT_SOURCING=false is no longer supported for ontology writes.",
            code=ErrorCode.INTERNAL_ERROR,
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in create_ontology: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        from oms.services.async_terminus import DuplicateOntologyError, OntologyNotFoundError, OntologyValidationError
        
        traceback.format_exc()
        
        # 에러 타입에 따른 적절한 HTTP 상태 코드 반환
        if isinstance(e, DuplicateOntologyError) or "DocumentIdAlreadyExists" in str(e):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                f"온톨로지 '{ontology_data.get('id')}'이(가) 이미 존재합니다",
                code=ErrorCode.ONTOLOGY_DUPLICATE,
            )
        elif isinstance(e, OntologyValidationError):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"온톨로지 검증 실패: {str(e)}",
                code=ErrorCode.ONTOLOGY_VALIDATION_FAILED,
            )
        elif isinstance(e, OntologyNotFoundError):
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                str(e),
                code=ErrorCode.ONTOLOGY_NOT_FOUND,
            )
        else:
            # 그 외의 경우에만 500 반환
            raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.post("/validate")
@trace_endpoint("oms.ontology.validate_create")
async def validate_ontology_create(
    ontology_request: OntologyCreateRequest,
    request: Request,
    db_name: str = Path(..., description="Database name"),
    branch: str = Query("main", description="Target branch (default: main)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
) -> Dict[str, Any]:
    """온톨로지 생성 검증 (no write)."""
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        lang = get_accept_language(request)

        if not await terminus.database_exists(db_name):
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        payload = ontology_request.model_dump()
        raw_label = payload.get("label") or payload.get("rdfs:label") or payload.get("id") or ""

        if payload.get("id"):
            class_id = validate_class_id(payload.get("id"))
            id_generated = False
        else:
            from shared.utils.id_generator import generate_simple_id

            class_id = generate_simple_id(
                label=raw_label,
                use_timestamp_for_korean=True,
                default_fallback="UnnamedClass",
            )
            id_generated = True

        label = _localized_to_string(raw_label, lang=lang) or class_id

        metadata_payload = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        expanded_properties, shared_prop_issues = await _apply_shared_properties(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=list(ontology_request.properties or []),
            metadata=metadata_payload,
        )
        value_type_issues = await _validate_value_type_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=expanded_properties,
        )
        missing_groups = await _validate_group_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            metadata=metadata_payload,
        )

        lint_report = lint_ontology_create(
            class_id=class_id,
            label=label,
            abstract=bool(payload.get("abstract", False)),
            properties=expanded_properties,
            relationships=list(ontology_request.relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=class_id,
            metadata=metadata_payload,
            properties=expanded_properties,
            relationships=list(ontology_request.relationships or []),
        )
        return ApiResponse.success(
            message="온톨로지 스키마 검증 결과입니다",
            data={
                "db_name": db_name,
                "branch": branch,
                "class_id": class_id,
                "id_generated": id_generated,
                "lint_report": lint_report.model_dump(),
                "interface_issues": interface_issues,
                "shared_property_issues": shared_prop_issues,
                "value_type_issues": value_type_issues,
                "missing_groups": missing_groups,
            },
        ).to_dict()
    except SecurityViolationError as e:
        logger.warning(f"Security violation in validate_ontology_create: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate ontology create: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.post("/{class_id}/validate")
@trace_endpoint("oms.ontology.validate_update")
async def validate_ontology_update(
    ontology_data: OntologyUpdateRequest,
    request: Request,
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
) -> Dict[str, Any]:
    """온톨로지 업데이트 검증 (no write)."""
    try:
        branch = validate_branch_name(branch)
        lang = get_accept_language(request)

        existing = await terminus.get_ontology(db_name, class_id, branch=branch)
        if not existing:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"온톨로지 '{class_id}'를 찾을 수 없습니다",
                code=ErrorCode.ONTOLOGY_NOT_FOUND,
            )

        patch = sanitize_input(ontology_data.model_dump(mode="json", exclude_unset=True))

        updated_properties = ontology_data.properties if ontology_data.properties is not None else existing.properties
        updated_relationships = (
            ontology_data.relationships if ontology_data.relationships is not None else existing.relationships
        )
        updated_abstract = ontology_data.abstract if ontology_data.abstract is not None else existing.abstract

        raw_label = patch.get("label") if "label" in patch else existing.label
        label = _localized_to_string(raw_label, lang=lang) or str(existing.label)

        if "metadata" in patch:
            metadata_payload = patch.get("metadata") if isinstance(patch.get("metadata"), dict) else {}
        else:
            metadata_payload = existing.metadata if isinstance(existing.metadata, dict) else {}

        expanded_properties, shared_prop_issues = await _apply_shared_properties(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=list(updated_properties or []),
            metadata=metadata_payload,
        )
        value_type_issues = await _validate_value_type_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=expanded_properties,
        )
        missing_groups = await _validate_group_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            metadata=metadata_payload,
        )

        baseline = lint_ontology_create(
            class_id=class_id,
            label=label,
            abstract=bool(updated_abstract),
            properties=expanded_properties,
            relationships=list(updated_relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        diff = lint_ontology_update(
            existing_properties=list(existing.properties or []),
            existing_relationships=list(existing.relationships or []),
            updated_properties=expanded_properties,
            updated_relationships=list(updated_relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        merged = _merge_lint_reports(baseline, diff)

        high_risk = any((issue.rule_id or "").startswith("ONT9") for issue in diff.warnings or [])
        protected_branch = _is_protected_branch(branch)

        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=class_id,
            metadata=metadata_payload,
            properties=expanded_properties,
            relationships=list(updated_relationships or []),
        )

        return ApiResponse.success(
            message="온톨로지 스키마 검증 결과입니다",
            data={
                "db_name": db_name,
                "branch": branch,
                "class_id": class_id,
                "protected_branch": protected_branch,
                "requires_proof": bool(protected_branch and high_risk),
                "lint_report": merged.model_dump(),
                "lint_report_create": baseline.model_dump(),
                "lint_report_diff": diff.model_dump(),
                "interface_issues": interface_issues,
                "shared_property_issues": shared_prop_issues,
                "value_type_issues": value_type_issues,
                "missing_groups": missing_groups,
            },
        ).to_dict()
    except SecurityViolationError as e:
        logger.warning(f"Security violation in validate_ontology_update: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate ontology update: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.get("")
@trace_endpoint("oms.ontology.list")
async def list_ontologies(
    db_name: str = Depends(ensure_database_exists),
    branch: str = Query("main", description="Target branch (default: main)"),
    class_type: str = "sys:Class",
    limit: Optional[int] = 100,
    offset: int = 0,
    terminus: AsyncTerminusService = TerminusServiceDep,
    label_mapper=LabelMapperDep,
):
    """내부 ID 기반 온톨로지 목록 조회"""
    try:
        branch = validate_branch_name(branch)

        # 페이징 파라미터 검증
        if limit is not None and (limit < 1 or limit > 1000):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "limit은 1-1000 범위여야 합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if offset < 0:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "offset은 0 이상이어야 합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # TerminusDB에서 조회 (branch-aware)
        effective_limit = limit if limit is not None else 1000
        ontologies = await terminus.get_ontology(
            db_name,
            class_id=None,
            raise_if_missing=False,
            branch=branch,
        )
        if ontologies:
            ontologies = ontologies[offset : offset + effective_limit]
        else:
            ontologies = []

        # 레이블 적용 (다국어 지원)
        labeled_ontologies = []
        if ontologies:
            try:
                normalized_ontologies = [
                    item.model_dump(mode="json") if hasattr(item, "model_dump") else item
                    for item in ontologies
                ]
                labeled_ontologies = await label_mapper.convert_to_display_batch(
                    db_name, normalized_ontologies, "ko"
                )
            except Exception as e:
                logger.warning(f"Failed to apply labels: {e}")
                labeled_ontologies = ontologies  # 레이블 적용 실패 시 원본 데이터 반환

        return {
            "status": "success",
            "message": f"온톨로지 목록 조회 완료 ({len(labeled_ontologies)}개)",
            "data": {
                "ontologies": labeled_ontologies,
                "count": len(labeled_ontologies),
                "limit": limit,
                "offset": offset,
                "branch": branch,
            },
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in list_ontologies: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list ontologies: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.get("/analyze-network")
@trace_endpoint("oms.ontology.analyze_network")
async def analyze_relationship_network(
    db_name: str = Depends(ensure_database_exists),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    🔥 관계 네트워크 종합 분석 엔드포인트

    전체 관계 네트워크의 건강성과 통계를 분석
    """
    try:
        # 네트워크 분석 수행
        analysis_result = await terminus.analyze_relationship_network(db_name)

        return {
            "status": "success",
            "message": "관계 네트워크 분석이 완료되었습니다",
            "data": analysis_result,
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in analyze_relationship_network: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except Exception as e:
        logger.error(f"Failed to analyze relationship network: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.get("/{class_id}")
@trace_endpoint("oms.ontology.get")
async def get_ontology(
    request: Request,
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
    converter: JSONToJSONLDConverter = JSONLDConverterDep,
    label_mapper=LabelMapperDep,
):
    """내부 ID 기반 온톨로지 조회"""
    try:
        branch = validate_branch_name(branch)

        # TerminusDB에서 조회
        ontology = await terminus.get_ontology(db_name, class_id, branch=branch)

        if not ontology:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"온톨로지 '{class_id}'를 찾을 수 없습니다",
                code=ErrorCode.ONTOLOGY_NOT_FOUND,
            )

        if _is_internal_ontology(ontology) and not _admin_authorized(request):
            raise classified_http_exception(
                status.HTTP_403_FORBIDDEN,
                "Internal ontology schema requires admin access",
                code=ErrorCode.PERMISSION_DENIED,
            )

        # JSON-LD를 일반 JSON으로 변환
        result = converter.convert_from_jsonld(ontology)
        
        # TerminusDB에서 가져온 데이터는 스키마와 메타데이터가 결합되어 있음
        # 이제 임시 해결책이 필요하지 않음
        
        # OntologyResponse 필수 필드 보장
        if "id" not in result:
            result["id"] = class_id
        if "properties" not in result:
            result["properties"] = []
        if "relationships" not in result:
            result["relationships"] = []

        # ApiResponse 사용 (올바른 표준 형식)
        from shared.models.responses import ApiResponse
        return ApiResponse.success(
            message=f"온톨로지 '{class_id}'를 조회했습니다", 
            data={**result, "branch": branch}
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_ontology: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        # Check for not found errors in exception message
        error_msg = str(e).lower()
        if (
            "not found" in error_msg
            or "찾을 수 없습니다" in str(e)
            or "does not exist" in error_msg
            or "documentnotfound" in error_msg
        ):
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"온톨로지 '{class_id}'를 찾을 수 없습니다",
                code=ErrorCode.ONTOLOGY_NOT_FOUND,
            )

        logger.error(f"Failed to get ontology: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.put(
    "/{class_id}",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_400_BAD_REQUEST: {"model": ApiResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Ontology not found"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
@trace_endpoint("oms.ontology.update")
async def update_ontology(
    ontology_data: OntologyUpdateRequest,
    request: Request,
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
    event_store=EventStoreDep,
    command_status_service=CommandStatusServiceDep,
) -> ApiResponse:
    """내부 ID 기반 온톨로지 업데이트"""
    try:
        enable_event_sourcing = bool(get_settings().event_sourcing.enable_event_sourcing)
        branch = validate_branch_name(branch)
        _reject_direct_write_if_required(branch)
        lang = get_accept_language(request)

        # 요청 데이터 정화
        sanitized_data = sanitize_input(ontology_data.model_dump(mode="json", exclude_unset=True))

        # 기존 데이터 조회
        existing = await terminus.get_ontology(db_name, class_id, branch=branch)

        if not existing:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"온톨로지 '{class_id}'를 찾을 수 없습니다",
                code=ErrorCode.ONTOLOGY_NOT_FOUND,
            )

        # Preserve EN/KR localized fields (string or language map) end-to-end.
        if "label" in sanitized_data:
            label_value = sanitized_data.get("label")
            if isinstance(label_value, (str, dict)):
                sanitized_data["label"] = coerce_localized_text(label_value)

        if "description" in sanitized_data:
            description_value = sanitized_data.get("description")
            if description_value is None:
                # Explicit null means "clear".
                sanitized_data["description"] = None
            elif isinstance(description_value, (str, dict)):
                sanitized_data["description"] = coerce_localized_text(description_value)

        updated_properties = ontology_data.properties if ontology_data.properties is not None else existing.properties
        updated_relationships = (
            ontology_data.relationships if ontology_data.relationships is not None else existing.relationships
        )
        updated_abstract = ontology_data.abstract if ontology_data.abstract is not None else existing.abstract

        if "label" in sanitized_data:
            label_for_lint = select_localized_text(sanitized_data.get("label"), lang=lang) or class_id
        else:
            label_for_lint = select_localized_text(existing.label, lang=lang) or class_id

        if "metadata" in sanitized_data:
            metadata_payload = (
                sanitized_data.get("metadata")
                if isinstance(sanitized_data.get("metadata"), dict)
                else {}
            )
        else:
            metadata_payload = existing.metadata if isinstance(existing.metadata, dict) else {}

        expanded_properties, shared_prop_issues = await _apply_shared_properties(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=list(updated_properties or []),
            metadata=metadata_payload,
        )
        if shared_prop_issues:
            error_payload = build_error_envelope(
                service_name="oms",
                message="공유 속성(shared property) 적용에 실패했습니다",
                detail="Shared property expansion failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=["Shared property expansion failed"],
                context={"shared_property_issues": shared_prop_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        value_type_issues = await _validate_value_type_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=expanded_properties,
        )
        if value_type_issues:
            issue_messages = [issue.get("message") for issue in value_type_issues]
            error_payload = build_error_envelope(
                service_name="oms",
                message="값 타입(value type) 검증에 실패했습니다",
                detail="Value type validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=issue_messages,
                context={"value_type_issues": value_type_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        missing_groups = await _validate_group_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            metadata=metadata_payload,
        )
        if missing_groups:
            error_payload = build_error_envelope(
                service_name="oms",
                message="그룹(group) 참조를 찾을 수 없습니다",
                detail="Group references not found",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=["Group references not found"],
                context={"missing_groups": missing_groups},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        converted_properties_raw = [
            p.model_dump() if hasattr(p, "model_dump") else p for p in expanded_properties
        ]
        converted_relationships = list(updated_relationships or [])
        if ontology_data.properties is not None or ontology_data.relationships is not None:
            conversion_payload = {
                "id": class_id,
                "properties": converted_properties_raw,
                "relationships": converted_relationships,
            }
            try:
                converted = _PROPERTY_CONVERTER.process_class_data(conversion_payload)
            except Exception as e:
                logger.error("Property→relationship conversion failed: %s", e)
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    f"Failed to convert properties to relationships: {e}",
                    code=ErrorCode.ONTOLOGY_RELATIONSHIP_ERROR,
                )
            converted_properties_raw = converted.get("properties") or []
            converted_relationships = converted.get("relationships") or []
            expanded_properties = [
                p if isinstance(p, Property) else Property(**p) for p in converted_properties_raw
            ]
            updated_relationships = converted_relationships

        baseline = lint_ontology_create(
            class_id=class_id,
            label=str(label_for_lint or class_id),
            abstract=bool(updated_abstract),
            properties=expanded_properties,
            relationships=list(updated_relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        diff = lint_ontology_update(
            existing_properties=list(existing.properties or []),
            existing_relationships=list(existing.relationships or []),
            updated_properties=expanded_properties,
            updated_relationships=list(updated_relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        merged_lint = _merge_lint_reports(baseline, diff)

        if not merged_lint.ok:
            lint_errors = [issue.message for issue in merged_lint.errors]
            error_payload = build_error_envelope(
                service_name="oms",
                message="온톨로지 스키마 검증에 실패했습니다",
                detail="Ontology schema validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=lint_errors,
                context={"lint_report": merged_lint.model_dump()},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=class_id,
            metadata=metadata_payload,
            properties=expanded_properties,
            relationships=list(updated_relationships or []),
        )
        if interface_issues:
            issue_messages = [issue.get("message") for issue in interface_issues]
            error_payload = build_error_envelope(
                service_name="oms",
                message="온톨로지 인터페이스 계약 검증에 실패했습니다",
                detail="Ontology interface contract validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=issue_messages,
                context={"interface_issues": interface_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        relationship_payload = {
            "id": class_id,
            "label": label_for_lint or class_id,
            "properties": converted_properties_raw,
            "relationships": converted_relationships,
        }
        relationship_response = await _validate_relationships_gate(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_payload=relationship_payload,
            enabled=_relationship_validation_enabled(),
        )
        if relationship_response:
            return relationship_response

        shared_refs = _extract_shared_property_refs(metadata_payload)
        if ontology_data.properties is not None or shared_refs:
            sanitized_data["properties"] = converted_properties_raw
            sanitized_data["relationships"] = converted_relationships

        protected_branch = _is_protected_branch(branch)
        high_risk = any((issue.rule_id or "").startswith("ONT9") for issue in diff.warnings or [])
        if protected_branch and high_risk:
            if not _extract_change_reason(request):
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    protected_branch_write_message(),
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )
            if not _admin_authorized(request):
                raise classified_http_exception(
                    status.HTTP_403_FORBIDDEN,
                    protected_branch_write_message(),
                    code=ErrorCode.PERMISSION_DENIED,
                )

        if enable_event_sourcing:
            ontology_version = await resolve_ontology_version(
                terminus, db_name=db_name, branch=branch, logger=logger
            )
            actor = _extract_actor(request)
            change_reason = _extract_change_reason(request)
            # Event Sourcing: publish UPDATE command (actual write is async in worker)
            command = OntologyCommand(
                command_type=CommandType.UPDATE_ONTOLOGY_CLASS,
                aggregate_id=f"{db_name}:{branch}:{class_id}",
                db_name=db_name,
                branch=branch,
                expected_seq=expected_seq,
                payload={
                    "db_name": db_name,
                    "branch": branch,
                    "class_id": class_id,
                    "updates": sanitized_data,
                },
                metadata={
                    "source": "OMS",
                    "user": actor or "system",
                    "ontology": ontology_version,
                    "change_reason": change_reason,
                    "lint": merged_lint.model_dump(),
                },
                created_by=actor,
            )

            await append_event_sourcing_command(
                event_store=event_store,
                command=command,
                actor=actor,
                kafka_topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC,
                command_status_service=command_status_service,
                command_status_metadata=build_command_status_metadata(
                    command=command,
                    extra={
                        "db_name": db_name,
                        "branch": branch,
                        "class_id": class_id,
                    },
                ),
            )

            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content=ApiResponse.accepted(
                    message=f"온톨로지 '{class_id}' 업데이트 명령이 접수되었습니다",
                    data={
                        "command_id": str(command.command_id),
                        "ontology_id": class_id,
                        "database": db_name,
                        "branch": branch,
                        "status": "processing",
                        "mode": "event_sourcing",
                    },
                ).to_dict(),
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "ENABLE_EVENT_SOURCING=false is no longer supported for ontology writes.",
            code=ErrorCode.INTERNAL_ERROR,
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in update_ontology: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update ontology: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.delete("/{class_id}", response_model=BaseResponse)
@trace_endpoint("oms.ontology.delete")
async def delete_ontology(
    request: Request,
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
    event_store=EventStoreDep,
    command_status_service=CommandStatusServiceDep,
):
    """내부 ID 기반 온톨로지 삭제"""
    try:
        enable_event_sourcing = bool(get_settings().event_sourcing.enable_event_sourcing)
        branch = validate_branch_name(branch)
        _reject_direct_write_if_required(branch)

        protected_branch = _is_protected_branch(branch)
        if protected_branch:
            if not _extract_change_reason(request):
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    protected_branch_write_message(),
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )
            if not _admin_authorized(request):
                raise classified_http_exception(
                    status.HTTP_403_FORBIDDEN,
                    protected_branch_write_message(),
                    code=ErrorCode.PERMISSION_DENIED,
                )

        # Best-effort command-side validation:
        # - Safe on main branch.
        # - For non-main branches, the authoritative existence check is in the worker (branch-aware),
        #   so we avoid false negatives due to stale caches or missing branch support.
        if branch == "main":
            existing = await terminus.get_ontology(db_name, class_id, branch=branch)
            if not existing:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    f"온톨로지 '{class_id}'를 찾을 수 없습니다",
                    code=ErrorCode.ONTOLOGY_NOT_FOUND,
                )

        if enable_event_sourcing:
            ontology_version = await resolve_ontology_version(
                terminus, db_name=db_name, branch=branch, logger=logger
            )
            actor = _extract_actor(request)
            change_reason = _extract_change_reason(request)
            command = OntologyCommand(
                command_type=CommandType.DELETE_ONTOLOGY_CLASS,
                aggregate_id=f"{db_name}:{branch}:{class_id}",
                db_name=db_name,
                branch=branch,
                expected_seq=expected_seq,
                payload={"db_name": db_name, "branch": branch, "class_id": class_id},
                metadata={
                    "source": "OMS",
                    "user": actor or "system",
                    "ontology": ontology_version,
                    "change_reason": change_reason,
                },
                created_by=actor,
            )

            await append_event_sourcing_command(
                event_store=event_store,
                command=command,
                actor=actor,
                kafka_topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC,
                command_status_service=command_status_service,
                command_status_metadata=build_command_status_metadata(
                    command=command,
                    extra={
                        "db_name": db_name,
                        "branch": branch,
                        "class_id": class_id,
                    },
                ),
            )

            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content=ApiResponse.accepted(
                    message=f"온톨로지 '{class_id}' 삭제 명령이 접수되었습니다",
                    data={
                        "command_id": str(command.command_id),
                        "ontology_id": class_id,
                        "database": db_name,
                        "branch": branch,
                        "status": "processing",
                        "mode": "event_sourcing",
                    },
                ).to_dict(),
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "ENABLE_EVENT_SOURCING=false is no longer supported for ontology writes.",
            code=ErrorCode.INTERNAL_ERROR,
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in delete_ontology: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete ontology: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.post("/query", response_model=QueryResponse)
@trace_endpoint("oms.ontology.query")
async def query_ontologies(
    query: QueryRequestInternal,
    db_name: str = Depends(ValidatedDatabaseName),
    branch: str = Query("main", description="Target branch (default: main)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """내부 ID 기반 온톨로지 쿼리"""
    try:
        # 쿼리 데이터 정화
        sanitized_query = sanitize_input(query.model_dump(mode="json"))
        branch = validate_branch_name(branch)

        # 클래스 ID 검증 (있는 경우)
        if sanitized_query.get("class_id"):
            sanitized_query["class_id"] = validate_class_id(sanitized_query["class_id"])

        # 페이징 파라미터 검증
        limit = sanitized_query.get("limit") or 50
        offset = sanitized_query.get("offset") or 0
        
        # 타입 검증 및 변환
        try:
            limit = int(limit) if limit is not None else 50
            offset = int(offset) if offset is not None else 0
        except (ValueError, TypeError):
            limit = 50
            offset = 0
            
        if limit < 1 or limit > 1000:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "limit은 1-1000 범위여야 합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if offset < 0:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "offset은 0 이상이어야 합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 데이터베이스 존재 여부 확인
        await _ensure_database_exists(db_name, terminus)

        # class_label을 class_id로 매핑 (하위 호환성)
        class_id = sanitized_query.get("class_id")
        if not class_id and sanitized_query.get("class_label"):
            class_id = sanitized_query.get("class_label")

        # 쿼리 딕셔너리 변환
        query_dict = {
            "class_id": class_id,
            "filters": [
                {
                    "field": sanitize_input(f.get("field", "")),
                    "operator": sanitize_input(f.get("operator", "")),
                    "value": sanitize_input(f.get("value", "")),
                }
                for f in sanitized_query.get("filters", [])
            ],
            "select": sanitized_query.get("select", []),
            "limit": limit,
            "offset": offset,
        }

        # 쿼리 실행
        result = await terminus.execute_query(db_name, query_dict, branch=branch)

        return {
            "status": "success",
            "message": "쿼리가 성공적으로 실행되었습니다",
            "data": result.get("results", []),
            "count": result.get("total", 0),
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in query_ontologies: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


# 🔥 THINK ULTRA! Enhanced Relationship Management Endpoints


@router.post(
    "/create-advanced",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_400_BAD_REQUEST: {"model": ApiResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Database not found"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
@rate_limit(**RateLimitPresets.WRITE)
@trace_endpoint("oms.ontology.create_advanced")
async def create_ontology_with_advanced_relationships(
    ontology_request: OntologyCreateRequest,
    request: Request,
    db_name: str = Path(..., description="Database name"),
    branch: str = Query("main", description="Target branch (default: main)"),
    auto_generate_inverse: bool = Query(False, description="(Not implemented) Auto-generate inverse metadata"),
    validate_relationships: bool = Query(True, description="Validate relationships against current schema"),
    check_circular_references: bool = Query(True, description="Reject introducing critical schema cycles"),
    terminus: AsyncTerminusService = TerminusServiceDep,
    event_store=EventStoreDep,
    command_status_service=CommandStatusServiceDep,
) -> ApiResponse:
    """
    🔥 고급 관계 관리 기능을 포함한 온톨로지 생성

    Features:
    - 자동 역관계 생성
    - 관계 검증 및 무결성 체크
    - 순환 참조 탐지
    - 카디널리티 일관성 검증
    """
    try:
        if auto_generate_inverse:
            raise classified_http_exception(
                status.HTTP_501_NOT_IMPLEMENTED,
                "auto_generate_inverse is not implemented yet. TerminusDB schema documents discard "
                "per-property custom metadata, so inverse metadata needs a dedicated projection store.",
                code=ErrorCode.FEATURE_NOT_IMPLEMENTED,
            )
        enable_event_sourcing = bool(get_settings().event_sourcing.enable_event_sourcing)
        branch = validate_branch_name(branch)
        _reject_direct_write_if_required(branch)
        lang = get_accept_language(request)

        db_name = validate_db_name(db_name)
        if not await terminus.database_exists(db_name):
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        ontology_data = ontology_request.model_dump(mode="json")
        class_id = ontology_data.get("id")
        if class_id:
            ontology_data["id"] = validate_class_id(class_id)

        if not ontology_data.get("id"):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Ontology ID is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        raw_label = ontology_data.get("label", ontology_data.get("rdfs:label", ontology_data.get("id")))
        raw_description = ontology_data.get("description", ontology_data.get("rdfs:comment"))

        label_i18n = coerce_localized_text(raw_label)
        description_i18n = coerce_localized_text(raw_description) if raw_description is not None else {}

        label_display = select_localized_text(label_i18n, lang=lang) or str(ontology_data.get("id") or "Unknown")

        metadata_payload = (
            ontology_data.get("metadata")
            if isinstance(ontology_data.get("metadata"), dict)
            else {}
        )
        expanded_properties, shared_prop_issues = await _apply_shared_properties(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=list(ontology_request.properties or []),
            metadata=metadata_payload,
        )
        if shared_prop_issues:
            error_payload = build_error_envelope(
                service_name="oms",
                message="공유 속성(shared property) 적용에 실패했습니다",
                detail="Shared property expansion failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=["Shared property expansion failed"],
                context={"shared_property_issues": shared_prop_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        value_type_issues = await _validate_value_type_refs(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            properties=expanded_properties,
        )
        if value_type_issues:
            issue_messages = [issue.get("message") for issue in value_type_issues]
            error_payload = build_error_envelope(
                service_name="oms",
                message="값 타입(value type) 검증에 실패했습니다",
                detail="Value type validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=issue_messages,
                context={"value_type_issues": value_type_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        lint_report = lint_ontology_create(
            class_id=str(ontology_data.get("id")),
            label=label_display,
            abstract=bool(ontology_data.get("abstract", False)),
            properties=expanded_properties,
            relationships=list(ontology_request.relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        if not lint_report.ok:
            lint_errors = [issue.message for issue in lint_report.errors]
            error_payload = build_error_envelope(
                service_name="oms",
                message="온톨로지 스키마 검증에 실패했습니다",
                detail="Ontology schema validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=lint_errors,
                context={"lint_report": lint_report.model_dump()},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=str(ontology_data.get("id") or ""),
            metadata=metadata_payload,
            properties=expanded_properties,
            relationships=list(ontology_request.relationships or []),
        )
        if interface_issues:
            issue_messages = [issue.get("message") for issue in interface_issues]
            error_payload = build_error_envelope(
                service_name="oms",
                message="온톨로지 인터페이스 계약 검증에 실패했습니다",
                detail="Ontology interface contract validation failed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                errors=issue_messages,
                context={"interface_issues": interface_issues},
            )
            return JSONResponse(
                status_code=error_payload["http_status"],
                content=error_payload,
            )

        try:
            ontology_data["properties"] = [
                p.model_dump() if hasattr(p, "model_dump") else p for p in expanded_properties
            ]
            ontology_data = _PROPERTY_CONVERTER.process_class_data(ontology_data)
        except Exception as e:
            logger.error("Property→relationship conversion failed: %s", e)
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"Failed to convert properties to relationships: {e}",
                code=ErrorCode.ONTOLOGY_RELATIONSHIP_ERROR,
            )

        relationship_response = await _validate_relationships_gate(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_payload=ontology_data,
            enabled=_relationship_validation_enabled(validate_relationships),
        )
        if relationship_response:
            return relationship_response

        if enable_event_sourcing:
            ontology_version = await resolve_ontology_version(
                terminus, db_name=db_name, branch=branch, logger=logger
            )
            actor = _extract_actor(request)
            command = OntologyCommand(
                command_type=CommandType.CREATE_ONTOLOGY_CLASS,
                aggregate_id=f"{db_name}:{branch}:{ontology_data.get('id')}",
                db_name=db_name,
                branch=branch,
                expected_seq=0,
                payload={
                    "db_name": db_name,
                    "branch": branch,
                    "class_id": ontology_data.get("id"),
                    "label": label_i18n,
                    "description": description_i18n or None,
                    "properties": ontology_data.get("properties", []),
                    "relationships": ontology_data.get("relationships", []),
                    "parent_class": ontology_data.get("parent_class"),
                    "abstract": ontology_data.get("abstract", False),
                    "metadata": metadata_payload,
                    "advanced_options": {
                        "auto_generate_inverse": bool(auto_generate_inverse),
                        "validate_relationships": bool(validate_relationships),
                        "check_circular_references": bool(check_circular_references),
                    },
                },
                metadata={"source": "OMS", "user": "system", "ontology": ontology_version},
                created_by=actor,
            )

            await append_event_sourcing_command(
                event_store=event_store,
                command=command,
                actor=actor,
                kafka_topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC,
                envelope_metadata={"variant": "advanced"},
                command_status_service=command_status_service,
                command_status_metadata=build_command_status_metadata(
                    command=command,
                    extra={
                        "db_name": db_name,
                        "branch": branch,
                        "class_id": ontology_data.get("id"),
                        "advanced_options": command.payload.get("advanced_options"),
                    },
                ),
            )

            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content=ApiResponse.accepted(
                    message=f"온톨로지 '{ontology_data.get('id')}' 생성(advanced) 명령이 접수되었습니다",
                    data={
                        "command_id": str(command.command_id),
                        "ontology_id": ontology_data.get("id"),
                        "database": db_name,
                        "branch": branch,
                        "status": "processing",
                        "mode": "event_sourcing",
                        "advanced_options": command.payload.get("advanced_options"),
                    },
                ).to_dict(),
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "ENABLE_EVENT_SOURCING=false is no longer supported for ontology writes.",
            code=ErrorCode.INTERNAL_ERROR,
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in create_ontology_with_advanced_relationships: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback

        logger.error(f"Failed to create ontology with advanced relationships: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.post("/validate-relationships")
@trace_endpoint("oms.ontology.validate_relationships")
async def validate_ontology_relationships(
    request: OntologyCreateRequest,
    db_name: str = Path(..., description="Database name"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    🔥 온톨로지 관계 검증 전용 엔드포인트

    실제 생성 없이 관계의 유효성만 검증
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 요청 데이터를 dict로 변환
        ontology_data = request.model_dump()

        # 클래스 ID 검증
        class_id = ontology_data.get("id")
        if class_id:
            ontology_data["id"] = validate_class_id(class_id)

        # 데이터베이스 존재 여부 확인
        await _ensure_database_exists(db_name, terminus)

        # 관계 검증 수행
        validation_result = await terminus.validate_relationships(db_name, ontology_data)

        return {
            "status": "success",
            "message": "관계 검증이 완료되었습니다",
            "data": validation_result,
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in validate_ontology_relationships: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except Exception as e:
        logger.error(f"Failed to validate relationships: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.post("/detect-circular-references")
@trace_endpoint("oms.ontology.detect_circular_references")
async def detect_circular_references(
    db_name: str = Path(..., description="Database name"),
    new_ontology: Optional[OntologyCreateRequest] = None,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    🔥 순환 참조 탐지 전용 엔드포인트

    기존 온톨로지들과 새 온톨로지(선택사항) 간의 순환 참조 탐지
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 데이터베이스 존재 여부 확인
        await _ensure_database_exists(db_name, terminus)

        # 새 온톨로지 데이터 준비
        new_ontology_data = None
        if new_ontology:
            new_ontology_data = new_ontology.model_dump()
            class_id = new_ontology_data.get("id")
            if class_id:
                new_ontology_data["id"] = validate_class_id(class_id)

        # 순환 참조 탐지 수행
        cycle_result = await terminus.detect_circular_references(
            db_name, include_new_ontology=new_ontology_data
        )

        return {
            "status": "success",
            "message": "순환 참조 탐지가 완료되었습니다",
            "data": cycle_result,
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in detect_circular_references: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except Exception as e:
        logger.error(f"Failed to detect circular references: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.get("/relationship-paths/{start_entity}")
@trace_endpoint("oms.ontology.find_relationship_paths")
async def find_relationship_paths(
    start_entity: str,
    db_name: str = Path(..., description="Database name"),
    end_entity: Optional[str] = None,
    max_depth: int = 5,
    path_type: str = "shortest",
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    🔥 관계 경로 탐색 엔드포인트

    엔티티 간의 관계 경로를 찾아 반환
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        start_entity = validate_class_id(start_entity)
        if end_entity:
            end_entity = validate_class_id(end_entity)

        # 파라미터 검증
        if max_depth < 1 or max_depth > 10:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "max_depth는 1-10 범위여야 합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 데이터베이스 존재 여부 확인
        await _ensure_database_exists(db_name, terminus)

        # 경로 탐색 수행
        path_result = await terminus.find_relationship_paths(
            db_name=db_name,
            start_entity=start_entity,
            end_entity=end_entity,
            max_depth=max_depth,
            path_type=path_type,
        )

        return {
            "status": "success",
            "message": f"관계 경로 탐색이 완료되었습니다 ({len(path_result.get('paths', []))}개 경로 발견)",
            "data": path_result,
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in find_relationship_paths: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except Exception as e:
        import traceback

        logger.error(f"Failed to find relationship paths: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.get("/reachable-entities/{start_entity}")
@trace_endpoint("oms.ontology.get_reachable_entities")
async def get_reachable_entities(
    start_entity: str,
    db_name: str = Path(..., description="Database name"),
    max_depth: int = 3,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    🔥 도달 가능한 엔티티 조회 엔드포인트

    시작 엔티티에서 도달 가능한 모든 엔티티 반환
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        start_entity = validate_class_id(start_entity)

        # 파라미터 검증
        if max_depth < 1 or max_depth > 5:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "max_depth는 1-5 범위여야 합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 데이터베이스 존재 여부 확인
        await _ensure_database_exists(db_name, terminus)

        # 도달 가능한 엔티티 조회
        reachable_result = await terminus.get_reachable_entities(
            db_name=db_name, start_entity=start_entity, max_depth=max_depth
        )

        return {
            "status": "success",
            "message": f"도달 가능한 엔티티 조회가 완료되었습니다 ({reachable_result.get('total_reachable', 0)}개 엔티티)",
            "data": reachable_result,
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_reachable_entities: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except Exception as e:
        logger.error(f"Failed to get reachable entities: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)
