"""
OMS 온톨로지 라우터 - 내부 ID 기반 온톨로지 관리
"""

import logging
import os
import hmac
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status, Path, Query
from fastapi.responses import JSONResponse

# Modernized dependency injection imports
from oms.dependencies import (
    get_jsonld_converter, 
    get_label_mapper, 
    get_terminus_service,
    TerminusServiceDep,
    JSONLDConverterDep,
    LabelMapperDep,
    EventStoreDep,  # Added for S3/MinIO Event Store
    CommandStatusServiceDep,
    ValidatedDatabaseName,
    ValidatedClassId,
    ensure_database_exists
)
from shared.models.commands import CommandType, OntologyCommand, CommandStatus
from shared.models.events import EventType
from shared.config.app_config import AppConfig
from shared.models.event_envelope import EventEnvelope
from shared.services.aggregate_sequence_allocator import OptimisticConcurrencyError
from shared.utils.ontology_version import resolve_ontology_version
from shared.utils.language import coerce_localized_text, get_accept_language, select_localized_text
from shared.services.ontology_linter import (
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
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.utils.label_mapper import LabelMapper
from shared.models.common import BaseResponse
from shared.models.requests import ApiResponse

# shared 모델 import
from shared.models.ontology import (
    OntologyBase,
    OntologyCreateRequest,
    OntologyResponse,
    OntologyUpdateRequest,
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


# Rate limiting import
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets
from shared.config.rate_limit_config import RateLimitConfig, EndpointCategory
from shared.security.auth_utils import extract_presented_token, get_expected_token
from shared.utils.branch_utils import get_protected_branches, protected_branch_write_message

logger = logging.getLogger(__name__)


def _is_protected_branch(branch: str) -> bool:
    return branch in get_protected_branches()


def _require_proposal_for_branch(branch: str) -> bool:
    if not _is_protected_branch(branch):
        return False
    return os.getenv("ONTOLOGY_REQUIRE_PROPOSALS", "true").strip().lower() in {"1", "true", "yes", "on"}


def _reject_direct_write_if_required(branch: str) -> None:
    if _require_proposal_for_branch(branch):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=protected_branch_write_message(),
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
) -> List[Dict[str, Any]]:
    refs = extract_interface_refs(metadata)
    if not refs:
        return []

    resource_service = OntologyResourceService(terminus)
    interface_index: Dict[str, Dict[str, Any]] = {}
    for raw_ref in refs:
        interface_id = strip_interface_prefix(raw_ref)
        if not interface_id:
            continue
        resource = await resource_service.get_resource(
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


async def _ensure_database_exists(db_name: str, terminus: AsyncTerminusService):
    """데이터베이스 존재 여부 확인 후 404 예외 발생"""
    # 데이터베이스 이름 보안 검증
    validated_db_name = validate_db_name(db_name)

    exists = await terminus.database_exists(validated_db_name)
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"데이터베이스 '{validated_db_name}'을(를) 찾을 수 없습니다",
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
    # 🔥 ULTRA DEBUG! OMS received data
    
    try:
        enable_event_sourcing = os.getenv("ENABLE_EVENT_SOURCING", "true").lower() == "true"
        branch = validate_branch_name(branch)
        _reject_direct_write_if_required(branch)
        lang = get_accept_language(request)

        # 🔥 FIXED: 데이터베이스 존재 확인 (dependency 제거로 인해 수동 처리)
        db_name = validate_db_name(db_name)
        if not await terminus.database_exists(db_name):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다"
            )
        
        # 요청 데이터를 dict로 변환
        ontology_data = ontology_request.model_dump()

        # 클래스 ID 검증
        class_id = ontology_data.get("id")
        if class_id:
            ontology_data["id"] = validate_class_id(class_id)

        # 기본 데이터 타입 검증
        if not ontology_data.get("id"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Ontology ID is required"
            )

        raw_label = ontology_data.get("label", ontology_data.get("rdfs:label", ontology_data.get("id")))
        raw_description = ontology_data.get("description", ontology_data.get("rdfs:comment"))

        label_i18n = coerce_localized_text(raw_label)
        description_i18n = coerce_localized_text(raw_description) if raw_description is not None else {}

        label_display = select_localized_text(label_i18n, lang=lang) or str(ontology_data.get("id") or "Unknown")
        description_display = (
            select_localized_text(description_i18n, lang=lang) if description_i18n else None
        )

        lint_report = lint_ontology_create(
            class_id=str(ontology_data.get("id")),
            label=label_display,
            abstract=bool(ontology_data.get("abstract", False)),
            properties=list(ontology_request.properties or []),
            relationships=list(ontology_request.relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        if not lint_report.ok:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=ApiResponse.error(
                    message="온톨로지 스키마 검증에 실패했습니다",
                    errors=[issue.message for issue in lint_report.errors],
                ).to_dict()
                | {"data": {"lint_report": lint_report.model_dump()}},
            )

        metadata_payload = (
            ontology_data.get("metadata")
            if isinstance(ontology_data.get("metadata"), dict)
            else {}
        )
        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=str(ontology_data.get("id") or ""),
            metadata=metadata_payload,
            properties=list(ontology_request.properties or []),
            relationships=list(ontology_request.relationships or []),
        )
        if interface_issues:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=ApiResponse.error(
                    message="온톨로지 인터페이스 계약 검증에 실패했습니다",
                    errors=[issue.get("message") for issue in interface_issues],
                ).to_dict()
                | {"data": {"interface_issues": interface_issues}},
            )

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

            envelope = EventEnvelope.from_command(
                command,
                actor=_extract_actor(request) or "system",
                kafka_topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC,
                metadata={"service": "oms", "mode": "event_sourcing"},
            )
            try:
                await event_store.append_event(envelope)
            except OptimisticConcurrencyError as e:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "optimistic_concurrency_conflict",
                        "aggregate_id": e.aggregate_id,
                        "expected_seq": e.expected_last_sequence,
                        "actual_seq": e.actual_last_sequence,
                    },
                )

            if command_status_service:
                try:
                    await command_status_service.set_command_status(
                        command_id=str(command.command_id),
                        status=CommandStatus.PENDING,
                        metadata={
                            "command_type": command.command_type,
                            "aggregate_id": command.aggregate_id,
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": ontology_data.get("id"),
                            "created_at": command.created_at.isoformat(),
                            "created_by": command.created_by or "system",
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to persist command status (continuing without Redis): {e}")

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

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ENABLE_EVENT_SOURCING=false is no longer supported for ontology writes.",
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in create_ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        from oms.services.async_terminus import DuplicateOntologyError, OntologyNotFoundError, OntologyValidationError
        
        error_msg = f"Failed to create ontology: {e}"
        traceback_str = traceback.format_exc()
        
        # 🔥 ULTRA! 에러 타입에 따른 적절한 HTTP 상태 코드 반환
        if isinstance(e, DuplicateOntologyError) or "DocumentIdAlreadyExists" in str(e):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"온톨로지 '{ontology_data.get('id')}'이(가) 이미 존재합니다"
            )
        elif isinstance(e, OntologyValidationError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"온톨로지 검증 실패: {str(e)}"
            )
        elif isinstance(e, OntologyNotFoundError):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e)
            )
        else:
            # 그 외의 경우에만 500 반환
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/validate")
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
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
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

        lint_report = lint_ontology_create(
            class_id=class_id,
            label=label,
            abstract=bool(payload.get("abstract", False)),
            properties=list(ontology_request.properties or []),
            relationships=list(ontology_request.relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        metadata_payload = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=class_id,
            metadata=metadata_payload,
            properties=list(ontology_request.properties or []),
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
            },
        ).to_dict()
    except SecurityViolationError as e:
        logger.warning(f"Security violation in validate_ontology_create: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate ontology create: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{class_id}/validate")
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
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'를 찾을 수 없습니다",
            )

        patch = sanitize_input(ontology_data.model_dump(mode="json", exclude_unset=True))

        updated_properties = ontology_data.properties if ontology_data.properties is not None else existing.properties
        updated_relationships = (
            ontology_data.relationships if ontology_data.relationships is not None else existing.relationships
        )
        updated_abstract = ontology_data.abstract if ontology_data.abstract is not None else existing.abstract

        raw_label = patch.get("label") if "label" in patch else existing.label
        label = _localized_to_string(raw_label, lang=lang) or str(existing.label)

        baseline = lint_ontology_create(
            class_id=class_id,
            label=label,
            abstract=bool(updated_abstract),
            properties=list(updated_properties or []),
            relationships=list(updated_relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        diff = lint_ontology_update(
            existing_properties=list(existing.properties or []),
            existing_relationships=list(existing.relationships or []),
            updated_properties=list(updated_properties or []),
            updated_relationships=list(updated_relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        merged = _merge_lint_reports(baseline, diff)

        high_risk = any((issue.rule_id or "").startswith("ONT9") for issue in diff.warnings or [])
        protected_branch = _is_protected_branch(branch)

        if "metadata" in patch:
            metadata_payload = patch.get("metadata") if isinstance(patch.get("metadata"), dict) else {}
        else:
            metadata_payload = existing.metadata if isinstance(existing.metadata, dict) else {}

        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=class_id,
            metadata=metadata_payload,
            properties=list(updated_properties or []),
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
            },
        ).to_dict()
    except SecurityViolationError as e:
        logger.warning(f"Security violation in validate_ontology_update: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate ontology update: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("")
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
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="limit은 1-1000 범위여야 합니다"
            )
        if offset < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="offset은 0 이상이어야 합니다"
            )

        # TerminusDB에서 조회 (branch-aware)
        ontologies = await terminus.get_ontology(db_name, branch=branch)

        # 레이블 적용 (다국어 지원)
        labeled_ontologies = []
        if ontologies:
            try:
                labeled_ontologies = await label_mapper.convert_to_display_batch(
                    db_name, ontologies, "ko"
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list ontologies: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/analyze-network")
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except Exception as e:
        logger.error(f"Failed to analyze relationship network: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{class_id}")
async def get_ontology(
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
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'를 찾을 수 없습니다",
            )

        if _is_internal_ontology(ontology) and not _admin_authorized(request):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Internal ontology schema requires admin access",
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
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
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'를 찾을 수 없습니다",
            )

        logger.error(f"Failed to get ontology: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


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
        enable_event_sourcing = os.getenv("ENABLE_EVENT_SOURCING", "true").lower() == "true"
        branch = validate_branch_name(branch)
        _reject_direct_write_if_required(branch)
        lang = get_accept_language(request)

        # 요청 데이터 정화
        sanitized_data = sanitize_input(ontology_data.model_dump(mode="json", exclude_unset=True))

        # 기존 데이터 조회
        existing = await terminus.get_ontology(db_name, class_id, branch=branch)

        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'를 찾을 수 없습니다",
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

        baseline = lint_ontology_create(
            class_id=class_id,
            label=str(label_for_lint or class_id),
            abstract=bool(updated_abstract),
            properties=list(updated_properties or []),
            relationships=list(updated_relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        diff = lint_ontology_update(
            existing_properties=list(existing.properties or []),
            existing_relationships=list(existing.relationships or []),
            updated_properties=list(updated_properties or []),
            updated_relationships=list(updated_relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        merged_lint = _merge_lint_reports(baseline, diff)

        if not merged_lint.ok:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=ApiResponse.error(
                    message="온톨로지 스키마 검증에 실패했습니다",
                    errors=[issue.message for issue in merged_lint.errors],
                ).to_dict()
                | {"data": {"lint_report": merged_lint.model_dump()}},
            )

        if "metadata" in sanitized_data:
            metadata_payload = (
                sanitized_data.get("metadata")
                if isinstance(sanitized_data.get("metadata"), dict)
                else {}
            )
        else:
            metadata_payload = existing.metadata if isinstance(existing.metadata, dict) else {}

        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=class_id,
            metadata=metadata_payload,
            properties=list(updated_properties or []),
            relationships=list(updated_relationships or []),
        )
        if interface_issues:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=ApiResponse.error(
                    message="온톨로지 인터페이스 계약 검증에 실패했습니다",
                    errors=[issue.get("message") for issue in interface_issues],
                ).to_dict()
                | {"data": {"interface_issues": interface_issues}},
            )

        protected_branch = _is_protected_branch(branch)
        high_risk = any((issue.rule_id or "").startswith("ONT9") for issue in diff.warnings or [])
        if protected_branch and high_risk:
            if not _extract_change_reason(request):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=protected_branch_write_message(),
                )
            if not _admin_authorized(request):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=protected_branch_write_message(),
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

            envelope = EventEnvelope.from_command(
                command,
                actor=actor or "system",
                kafka_topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC,
                metadata={"service": "oms", "mode": "event_sourcing"},
            )
            try:
                await event_store.append_event(envelope)
            except OptimisticConcurrencyError as e:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "optimistic_concurrency_conflict",
                        "aggregate_id": e.aggregate_id,
                        "expected_seq": e.expected_last_sequence,
                        "actual_seq": e.actual_last_sequence,
                    },
                )

            if command_status_service:
                try:
                    await command_status_service.set_command_status(
                        command_id=str(command.command_id),
                        status=CommandStatus.PENDING,
                        metadata={
                            "command_type": command.command_type,
                            "aggregate_id": command.aggregate_id,
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": class_id,
                            "created_at": command.created_at.isoformat(),
                            "created_by": command.created_by or "system",
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to persist command status (continuing without Redis): {e}")

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

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ENABLE_EVENT_SOURCING=false is no longer supported for ontology writes.",
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in update_ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update ontology: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.delete("/{class_id}", response_model=BaseResponse)
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
        enable_event_sourcing = os.getenv("ENABLE_EVENT_SOURCING", "true").lower() == "true"
        branch = validate_branch_name(branch)
        _reject_direct_write_if_required(branch)

        protected_branch = _is_protected_branch(branch)
        if protected_branch:
            if not _extract_change_reason(request):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=protected_branch_write_message(),
                )
            if not _admin_authorized(request):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=protected_branch_write_message(),
                )

        # Best-effort command-side validation:
        # - Safe on main branch.
        # - For non-main branches, the authoritative existence check is in the worker (branch-aware),
        #   so we avoid false negatives due to stale caches or missing branch support.
        if branch == "main":
            existing = await terminus.get_ontology(db_name, class_id, branch=branch)
            if not existing:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"온톨로지 '{class_id}'를 찾을 수 없습니다",
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

            envelope = EventEnvelope.from_command(
                command,
                actor=actor or "system",
                kafka_topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC,
                metadata={"service": "oms", "mode": "event_sourcing"},
            )
            try:
                await event_store.append_event(envelope)
            except OptimisticConcurrencyError as e:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "optimistic_concurrency_conflict",
                        "aggregate_id": e.aggregate_id,
                        "expected_seq": e.expected_last_sequence,
                        "actual_seq": e.actual_last_sequence,
                    },
                )

            if command_status_service:
                try:
                    await command_status_service.set_command_status(
                        command_id=str(command.command_id),
                        status=CommandStatus.PENDING,
                        metadata={
                            "command_type": command.command_type,
                            "aggregate_id": command.aggregate_id,
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": class_id,
                            "created_at": command.created_at.isoformat(),
                            "created_by": command.created_by or "system",
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to persist command status (continuing without Redis): {e}")

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

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ENABLE_EVENT_SOURCING=false is no longer supported for ontology writes.",
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in delete_ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete ontology: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/query", response_model=QueryResponse)
async def query_ontologies(
    query: QueryRequestInternal,
    db_name: str = Depends(ValidatedDatabaseName),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """내부 ID 기반 온톨로지 쿼리"""
    try:
        # 쿼리 데이터 정화
        sanitized_query = sanitize_input(query.model_dump(mode="json"))

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
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="limit은 1-1000 범위여야 합니다"
            )
        if offset < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="offset은 0 이상이어야 합니다"
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
        result = await terminus.execute_query(db_name, query_dict)

        return {
            "status": "success",
            "message": "쿼리가 성공적으로 실행되었습니다",
            "data": result.get("results", []),
            "count": result.get("total", 0),
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in query_ontologies: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


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
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail=(
                    "auto_generate_inverse is not implemented yet. TerminusDB schema documents discard "
                    "per-property custom metadata, so inverse metadata needs a dedicated projection store."
                ),
            )
        enable_event_sourcing = os.getenv("ENABLE_EVENT_SOURCING", "true").lower() == "true"
        branch = validate_branch_name(branch)
        _reject_direct_write_if_required(branch)
        lang = get_accept_language(request)

        db_name = validate_db_name(db_name)
        if not await terminus.database_exists(db_name):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다"
            )

        ontology_data = ontology_request.model_dump(mode="json")
        class_id = ontology_data.get("id")
        if class_id:
            ontology_data["id"] = validate_class_id(class_id)

        if not ontology_data.get("id"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ontology ID is required")

        raw_label = ontology_data.get("label", ontology_data.get("rdfs:label", ontology_data.get("id")))
        raw_description = ontology_data.get("description", ontology_data.get("rdfs:comment"))

        label_i18n = coerce_localized_text(raw_label)
        description_i18n = coerce_localized_text(raw_description) if raw_description is not None else {}

        label_display = select_localized_text(label_i18n, lang=lang) or str(ontology_data.get("id") or "Unknown")

        lint_report = lint_ontology_create(
            class_id=str(ontology_data.get("id")),
            label=label_display,
            abstract=bool(ontology_data.get("abstract", False)),
            properties=list(ontology_request.properties or []),
            relationships=list(ontology_request.relationships or []),
            config=OntologyLinterConfig.from_env(branch=branch),
        )
        if not lint_report.ok:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=ApiResponse.error(
                    message="온톨로지 스키마 검증에 실패했습니다",
                    errors=[issue.message for issue in lint_report.errors],
                ).to_dict()
                | {"data": {"lint_report": lint_report.model_dump()}},
            )

        metadata_payload = (
            ontology_data.get("metadata")
            if isinstance(ontology_data.get("metadata"), dict)
            else {}
        )
        interface_issues = await _collect_interface_issues(
            terminus=terminus,
            db_name=db_name,
            branch=branch,
            ontology_id=str(ontology_data.get("id") or ""),
            metadata=metadata_payload,
            properties=list(ontology_request.properties or []),
            relationships=list(ontology_request.relationships or []),
        )
        if interface_issues:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=ApiResponse.error(
                    message="온톨로지 인터페이스 계약 검증에 실패했습니다",
                    errors=[issue.get("message") for issue in interface_issues],
                ).to_dict()
                | {"data": {"interface_issues": interface_issues}},
            )

        if enable_event_sourcing:
            ontology_version = await resolve_ontology_version(
                terminus, db_name=db_name, branch=branch, logger=logger
            )
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
                created_by=_extract_actor(request),
            )

            envelope = EventEnvelope.from_command(
                command,
                actor=_extract_actor(request) or "system",
                kafka_topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC,
                metadata={"service": "oms", "mode": "event_sourcing", "variant": "advanced"},
            )
            try:
                await event_store.append_event(envelope)
            except OptimisticConcurrencyError as e:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "optimistic_concurrency_conflict",
                        "aggregate_id": e.aggregate_id,
                        "expected_seq": e.expected_last_sequence,
                        "actual_seq": e.actual_last_sequence,
                    },
                )

            if command_status_service:
                try:
                    await command_status_service.set_command_status(
                        command_id=str(command.command_id),
                        status=CommandStatus.PENDING,
                        metadata={
                            "command_type": command.command_type,
                            "aggregate_id": command.aggregate_id,
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": ontology_data.get("id"),
                            "created_at": command.created_at.isoformat(),
                            "created_by": command.created_by or "system",
                            "advanced_options": command.payload.get("advanced_options"),
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to persist command status (continuing without Redis): {e}")

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

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ENABLE_EVENT_SOURCING=false is no longer supported for ontology writes.",
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in create_ontology_with_advanced_relationships: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback

        logger.error(f"Failed to create ontology with advanced relationships: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/validate-relationships")
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except Exception as e:
        logger.error(f"Failed to validate relationships: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/detect-circular-references")
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except Exception as e:
        logger.error(f"Failed to detect circular references: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/relationship-paths/{start_entity}")
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
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="max_depth는 1-10 범위여야 합니다"
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except Exception as e:
        import traceback

        logger.error(f"Failed to find relationship paths: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/reachable-entities/{start_entity}")
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
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="max_depth는 1-5 범위여야 합니다"
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except Exception as e:
        logger.error(f"Failed to get reachable entities: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
