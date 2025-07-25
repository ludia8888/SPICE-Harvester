"""
OMS 온톨로지 라우터 - 내부 ID 기반 온톨로지 관리
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status

from oms.dependencies import (
    get_jsonld_converter, 
    get_label_mapper, 
    get_terminus_service,
    ValidatedDatabaseName,
    ValidatedClassId,
    ensure_database_exists
)

# OMS 서비스 import
from oms.services.async_terminus import AsyncTerminusService
from shared.models.common import BaseResponse

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
    validate_class_id,
    validate_db_name,
)

# shared utils import
from shared.utils.jsonld import JSONToJSONLDConverter

logger = logging.getLogger(__name__)


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


router = APIRouter(prefix="/ontology/{db_name}", tags=["Ontology Management"])


@router.post("/create", response_model=OntologyResponse)
async def create_ontology(
    db_name: str = Depends(ensure_database_exists),
    request: OntologyCreateRequest = ...,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
    converter: JSONToJSONLDConverter = Depends(get_jsonld_converter),
    label_mapper=Depends(get_label_mapper),
) -> OntologyResponse:
    """내부 ID 기반 온톨로지 생성"""
    # 🔥 ULTRA DEBUG! OMS received data
    print(f"🔥🔥🔥 OMS create_ontology called! db_name={db_name}")
    print(f"🔥🔥🔥 request data: {request}")
    logger.warning(f"🔥🔥🔥 OMS create_ontology called! db_name={db_name}, request={request}")
    
    try:
        # 요청 데이터를 dict로 변환
        ontology_data = request.model_dump()

        # 클래스 ID 검증
        class_id = ontology_data.get("id")
        if class_id:
            ontology_data["id"] = validate_class_id(class_id)

        # 기본 데이터 타입 검증
        if not ontology_data.get("id"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Ontology ID is required"
            )

        # TerminusDB에 직접 저장 (create_ontology_class 사용)
        result = await terminus.create_ontology_class(db_name, ontology_data)

        # 레이블 매핑 등록 (다국어 지원)
        class_id = ontology_data.get("id")
        if class_id:
            try:
                # 레이블 정보 추출 및 등록
                label_info = ontology_data.get("label", ontology_data.get("rdfs:label", class_id))
                description_info = ontology_data.get(
                    "description", ontology_data.get("rdfs:comment", "")
                )

                await label_mapper.register_class(db_name, class_id, label_info, description_info)

                # 속성 레이블 등록 (있는 경우)
                properties = ontology_data.get("properties", {})
                if isinstance(properties, dict):
                    for prop_name, prop_info in properties.items():
                        if isinstance(prop_info, dict) and "label" in prop_info:
                            await label_mapper.register_property(
                                db_name, class_id, prop_name, prop_info["label"]
                            )

                logger.info(f"Registered labels for ontology: {class_id}")
            except Exception as e:
                logger.warning(f"Failed to register labels for {class_id}: {e}")
                # 레이블 등록 실패는 온톨로지 생성을 실패시키지 않음

        # 레이블을 간단한 문자열로 처리
        label_data = ontology_data.get(
            "label", ontology_data.get("rdfs:label", ontology_data.get("id"))
        )
        if isinstance(label_data, dict):
            # 딕셔너리에서 적절한 언어의 문자열 추출
            label = label_data.get("en") or label_data.get("ko") or list(label_data.values())[0] if label_data else ontology_data.get("id", "Unknown")
        else:
            label = str(label_data) if label_data else ontology_data.get("id", "Unknown")

        # 설명을 간단한 문자열로 처리
        description_data = ontology_data.get("description", ontology_data.get("rdfs:comment"))
        description = None
        if description_data:
            if isinstance(description_data, dict):
                description = description_data.get("en") or description_data.get("ko") or list(description_data.values())[0] if description_data else None
            else:
                description = str(description_data)

        # 생성된 온톨로지 데이터를 OntologyResponse 형식으로 직접 변환
        return OntologyResponse(
            id=ontology_data.get("id"),
            label=label,
            description=description,
            properties=ontology_data.get("properties", []),
            relationships=ontology_data.get("relationships", []),
            parent_class=ontology_data.get("parent_class"),
            abstract=ontology_data.get("abstract", False),
            metadata={
                "terminus_response": result,  # 원본 TerminusDB 응답 보존
                "creation_timestamp": datetime.utcnow().isoformat(),
            },
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
        logger.error(f"🔥🔥🔥 ERROR in create_ontology: {error_msg}")
        logger.error(f"🔥🔥🔥 TRACEBACK:\n{traceback_str}")
        logger.error(f"🔥🔥🔥 ontology_data was: {ontology_data}")
        
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


@router.get("/list")
async def list_ontologies(
    db_name: str = Depends(ensure_database_exists),
    class_type: str = "sys:Class",
    limit: Optional[int] = 100,
    offset: int = 0,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
    label_mapper=Depends(get_label_mapper),
):
    """내부 ID 기반 온톨로지 목록 조회"""
    try:
        # 페이징 파라미터 검증
        if limit is not None and (limit < 1 or limit > 1000):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="limit은 1-1000 범위여야 합니다"
            )
        if offset < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="offset은 0 이상이어야 합니다"
            )

        # TerminusDB에서 조회
        ontologies = await terminus.list_ontology_classes(db_name)

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
    terminus: AsyncTerminusService = Depends(get_terminus_service),
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
    terminus: AsyncTerminusService = Depends(get_terminus_service),
    converter: JSONToJSONLDConverter = Depends(get_jsonld_converter),
    label_mapper=Depends(get_label_mapper),
):
    """내부 ID 기반 온톨로지 조회"""
    try:

        # TerminusDB에서 조회
        ontology = await terminus.get_ontology(db_name, class_id)

        if not ontology:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'를 찾을 수 없습니다",
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
            data=result
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


@router.put("/{class_id}", response_model=OntologyResponse)
async def update_ontology(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    ontology_data: OntologyUpdateRequest = ...,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
    converter: JSONToJSONLDConverter = Depends(get_jsonld_converter),
):
    """내부 ID 기반 온톨로지 업데이트"""
    try:
        # 요청 데이터 정화
        sanitized_data = sanitize_input(ontology_data.dict(exclude_unset=True))

        # 기존 데이터 조회
        existing = await terminus.get_ontology(db_name, class_id)

        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'를 찾을 수 없습니다",
            )

        # 업데이트 데이터 병합
        merged_data = {**converter.extract_from_jsonld(existing), **sanitized_data}
        merged_data["id"] = class_id  # ID는 변경 불가

        # JSON-LD로 변환
        jsonld_data = converter.convert_with_labels(merged_data)

        # TerminusDB 업데이트
        result = await terminus.update_ontology(db_name, class_id, jsonld_data)

        return OntologyResponse(
            status="success", message=f"온톨로지 '{class_id}'가 업데이트되었습니다", data=result
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
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    terminus: AsyncTerminusService = Depends(get_terminus_service)
):
    """내부 ID 기반 온톨로지 삭제"""
    try:

        # TerminusDB에서 삭제
        success = await terminus.delete_ontology(db_name, class_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'를 찾을 수 없습니다",
            )

        return BaseResponse(status="success", message=f"온톨로지 '{class_id}'가 삭제되었습니다")

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
    db_name: str = Depends(ValidatedDatabaseName),
    query: QueryRequestInternal = ...,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
):
    """내부 ID 기반 온톨로지 쿼리"""
    try:
        # 쿼리 데이터 정화
        sanitized_query = sanitize_input(query.dict())

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

        return QueryResponse(
            status="success",
            message="쿼리가 성공적으로 실행되었습니다",
            data=result.get("results", []),
            count=result.get("total", 0),
        )

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


@router.post("/create-advanced", response_model=OntologyResponse)
async def create_ontology_with_advanced_relationships(
    db_name: str,
    request: OntologyCreateRequest,
    auto_generate_inverse: bool = True,
    validate_relationships: bool = True,
    check_circular_references: bool = True,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
    label_mapper=Depends(get_label_mapper),
) -> OntologyResponse:
    """
    🔥 고급 관계 관리 기능을 포함한 온톨로지 생성

    Features:
    - 자동 역관계 생성
    - 관계 검증 및 무결성 체크
    - 순환 참조 탐지
    - 카디널리티 일관성 검증
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

        # 🔥 고급 관계 관리 기능으로 온톨로지 생성
        result = await terminus.create_ontology_with_advanced_relationships(
            db_name=db_name,
            ontology_data=ontology_data,
            auto_generate_inverse=auto_generate_inverse,
            validate_relationships=validate_relationships,
            check_circular_references=check_circular_references,
        )

        # 레이블 매핑 등록
        if class_id:
            try:
                label_info = ontology_data.get("label", class_id)
                description_info = ontology_data.get("description", "")
                await label_mapper.register_class(db_name, class_id, label_info, description_info)
                logger.info(f"Registered labels for advanced ontology: {class_id}")
            except Exception as e:
                logger.warning(f"Failed to register labels for {class_id}: {e}")

        # 레이블을 간단한 문자열로 처리
        label_data = ontology_data.get("label", class_id)
        if isinstance(label_data, dict):
            label = label_data.get("en") or label_data.get("ko") or list(label_data.values())[0] if label_data else class_id or "Unknown"
        else:
            label = str(label_data) if label_data else class_id or "Unknown"

        # 설명을 간단한 문자열로 처리
        description_data = ontology_data.get("description")
        description = None
        if description_data:
            if isinstance(description_data, dict):
                description = description_data.get("en") or description_data.get("ko") or list(description_data.values())[0] if description_data else None
            else:
                description = str(description_data)

        # OntologyResponse 를 직접 생성
        return OntologyResponse(
            id=class_id,
            label=label,
            description=description,
            properties=ontology_data.get("properties", []),
            relationships=ontology_data.get("relationships", []),
            parent_class=ontology_data.get("parent_class"),
            abstract=ontology_data.get("abstract", False),
            metadata={
                "terminus_response": result,
                "creation_timestamp": datetime.utcnow().isoformat(),
                "advanced_features": {
                    "auto_generate_inverse": auto_generate_inverse,
                    "validate_relationships": validate_relationships,
                    "check_circular_references": check_circular_references,
                },
            },
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
    db_name: str,
    request: OntologyCreateRequest,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
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
    db_name: str,
    new_ontology: Optional[OntologyCreateRequest] = None,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
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
    db_name: str,
    start_entity: str,
    end_entity: Optional[str] = None,
    max_depth: int = 5,
    path_type: str = "shortest",
    terminus: AsyncTerminusService = Depends(get_terminus_service),
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
    db_name: str,
    start_entity: str,
    max_depth: int = 3,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
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
