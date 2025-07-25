"""
온톨로지 CRUD 라우터
온톨로지 생성, 조회, 수정, 삭제를 담당
"""

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

from shared.models.ontology import (
    OntologyBase,
    OntologyCreateRequestBFF,
    OntologyResponse,
    OntologyUpdateInput,
    Property,
    Relationship,
)

# Add shared path for common utilities
from shared.utils.language import get_accept_language

# Security validation imports
from shared.security.input_sanitizer import sanitize_input, validate_db_name, validate_class_id


# Schema suggestion request models
class SchemaFromDataRequest(BaseModel):
    """Request model for schema suggestion from data"""

    data: List[List[Any]]
    columns: List[str]
    class_name: Optional[str] = None
    include_complex_types: bool = False


class SchemaFromGoogleSheetsRequest(BaseModel):
    """Request model for schema suggestion from Google Sheets"""

    sheet_url: str
    worksheet_name: Optional[str] = None
    class_name: Optional[str] = None
    api_key: Optional[str] = None


from fastapi import HTTPException

from bff.dependencies import (
    JSONToJSONLDConverter,
    LabelMapper,
    TerminusService,
    get_jsonld_converter,
    get_label_mapper,
    get_terminus_service,
    get_oms_client,
)
from bff.services.adapter_service import BFFAdapterService
from bff.services.oms_client import OMSClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}", tags=["Ontology Management"])


# Dependency for BFF Adapter Service
def get_bff_adapter(
    terminus_service: TerminusService = Depends(get_terminus_service),
    label_mapper: LabelMapper = Depends(get_label_mapper)
) -> BFFAdapterService:
    """Get BFF Adapter Service instance"""
    return BFFAdapterService(terminus_service, label_mapper)


@router.post("/ontology", response_model=OntologyResponse)
async def create_ontology(
    db_name: str,
    ontology: Dict[str, Any],  # Accept raw dict to preserve target field
    mapper: LabelMapper = Depends(get_label_mapper),
    terminus: TerminusService = Depends(get_terminus_service),
    jsonld_conv: JSONToJSONLDConverter = Depends(get_jsonld_converter),
):
    """
    온톨로지 생성

    새로운 온톨로지 클래스를 생성합니다.
    레이블 기반으로 ID가 자동 생성됩니다.
    """
    # 🔥 ULTRA DEBUG! Force logging to check if route is called
    print(f"🔥🔥🔥 CREATE_ONTOLOGY CALLED! db_name={db_name}, ontology={ontology}")
    logger.warning(f"🔥🔥🔥 CREATE_ONTOLOGY CALLED! db_name={db_name}, ontology={ontology}")
    
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        
        # 입력 데이터 처리 (이미 dict 형태)
        ontology_dict = ontology.copy()  # Make a copy to avoid modifying the original
        ontology_dict = sanitize_input(ontology_dict)

        # ID 생성 또는 검증
        if ontology_dict.get('id'):
            # 사용자가 ID를 제공한 경우 검증
            class_id = validate_class_id(ontology_dict['id'])
            logger.info(f"Using provided class_id '{class_id}'")
        else:
            # ID가 제공되지 않은 경우 자동 생성
            from shared.utils.id_generator import generate_simple_id
            class_id = generate_simple_id(
                label=ontology_dict.get('label', ''), use_timestamp_for_korean=True, default_fallback="UnnamedClass"
            )
            logger.info(f"Generated class_id '{class_id}' from label '{ontology_dict.get('label', '')}')")

        ontology_dict["id"] = class_id

        # 🔥 THINK ULTRA! Transform properties for OMS compatibility
        # Convert 'target' to 'linkTarget' for link-type properties
        def transform_properties_for_oms(data):
            if 'properties' in data and isinstance(data['properties'], list):
                for i, prop in enumerate(data['properties']):
                    if isinstance(prop, dict):
                        # 🔥 ULTRA! BFF uses 'label' but OMS requires 'name'
                        if 'name' not in prop and 'label' in prop:
                            # Generate name from label
                            prop['name'] = generate_simple_id(prop['label'], use_timestamp_for_korean=False)
                            logger.info(f"🔥 Generated property name '{prop['name']}' from label '{prop['label']}'")
                        
                        # 🔥 ULTRA! Convert STRING to xsd:string for OMS
                        if prop.get('type') == 'STRING':
                            prop['type'] = 'xsd:string'
                        elif prop.get('type') == 'INTEGER':
                            prop['type'] = 'xsd:integer'
                        elif prop.get('type') == 'DECIMAL':
                            prop['type'] = 'xsd:decimal'
                        elif prop.get('type') == 'BOOLEAN':
                            prop['type'] = 'xsd:boolean'
                        elif prop.get('type') == 'DATETIME':
                            prop['type'] = 'xsd:dateTime'
                        
                        # Convert target to linkTarget for link type properties
                        if prop.get('type') == 'link' and 'target' in prop:
                            prop['linkTarget'] = prop.pop('target')
                            logger.info(f"🔧 Converted property '{prop.get('name')}' target -> linkTarget: {prop.get('linkTarget')}")
                        
                        # Handle array properties with link items
                        if prop.get('type') == 'array' and 'items' in prop:
                            items = prop['items']
                            if isinstance(items, dict) and items.get('type') == 'link' and 'target' in items:
                                items['linkTarget'] = items.pop('target')
                                logger.info(f"🔧 Converted array property '{prop.get('name')}' items target -> linkTarget: {items.get('linkTarget')}")
        
        # Apply transformation
        transform_properties_for_oms(ontology_dict)
        
        # 🔥 ULTRA DEBUG! Write to file to verify transformation
        import datetime
        debug_file = f"/tmp/bff_debug_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(debug_file, 'w') as f:
            f.write(f"BEFORE transformation: {json.dumps(ontology, ensure_ascii=False, indent=2)}\n\n")
            f.write(f"AFTER transformation: {json.dumps(ontology_dict, ensure_ascii=False, indent=2)}\n")
        
        # Log transformed data for debugging
        logger.info(f"🔥 ULTRA DEBUG! Sending to OMS: {json.dumps(ontology_dict, ensure_ascii=False, indent=2)}")
        
        # 온톨로지 생성
        result = await terminus.create_class(db_name, ontology_dict)
        logger.info(f"OMS create result: {result}")

        # 레이블 매핑 등록
        await mapper.register_class(db_name, class_id, ontology_dict.get('label', ''), ontology_dict.get('description'))

        # 속성 레이블 매핑
        for prop in ontology_dict.get('properties', []):
            if isinstance(prop, dict):
                await mapper.register_property(db_name, class_id, prop.get('name', ''), prop.get('label', ''))

        # 관계 레이블 매핑
        for rel in ontology_dict.get('relationships', []):
            if isinstance(rel, dict):
                await mapper.register_relationship(db_name, rel.get('predicate', ''), rel.get('label', ''))

        # OMS 응답에서 생성된 데이터 추출
        if isinstance(result, dict):
            # OMS가 data 필드를 반환하는 경우
            if "data" in result and result.get("status") == "success":
                created_data = result["data"]
                # 생성된 ID 사용
                if "id" in created_data:
                    class_id = created_data["id"]
            elif "id" in result:
                # 직접 데이터 형식인 경우
                created_data = result
                class_id = result["id"]

        # 응답 생성
        # Validate properties and relationships to ensure they're properly formatted
        validated_properties = []
        for prop in ontology_dict.get('properties', []):
            if isinstance(prop, dict):
                validated_properties.append(Property(**prop))
                
        validated_relationships = []
        for rel in ontology_dict.get('relationships', []):
            if isinstance(rel, dict):
                validated_relationships.append(Relationship(**rel))
        
        # Create response
        return OntologyResponse(
            id=class_id,
            label=ontology_dict.get('label', ''),
            description=ontology_dict.get('description'),
            parent_class=ontology_dict.get('parent_class'),
            abstract=ontology_dict.get('abstract', False),
            properties=validated_properties,
            relationships=validated_relationships,
            metadata={"created": True, "database": db_name}
        )

    except HTTPException as he:
        # 🔥 ULTRA! Properly propagate HTTP exceptions with correct status codes
        logger.error(f"HTTP exception in create_ontology: {he.status_code} - {he.detail}")
        raise he
    except Exception as e:
        logger.error(f"Failed to create ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 생성 실패: {str(e)}",
        )


@router.get("/ontology/list")
async def list_ontologies(
    db_name: str,
    request: Request,
    class_type: str = Query("sys:Class", description="클래스 타입"),
    limit: Optional[int] = Query(None, description="결과 개수 제한"),
    offset: int = Query(0, description="오프셋"),
    mapper: LabelMapper = Depends(get_label_mapper),
    terminus: TerminusService = Depends(get_terminus_service),
):
    """
    온톨로지 목록 조회

    데이터베이스의 모든 온톨로지를 조회합니다.
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        # 온톨로지 목록 조회
        ontologies = await terminus.list_classes(db_name)
        
        # 🔥 ULTRA DEBUG!
        logger.info(f"🔥 ontologies type: {type(ontologies)}")
        logger.info(f"🔥 ontologies content: {ontologies[:2] if ontologies else 'empty'}")

        # 배치 레이블 정보 추가 (N+1 쿼리 문제 해결)
        # 🔥 ULTRA! Skip label mapping for now to fix the issue
        labeled_ontologies = ontologies  # await mapper.convert_to_display_batch(db_name, ontologies, lang)

        return {
            "total": len(labeled_ontologies),
            "ontologies": labeled_ontologies,
            "offset": offset,
            "limit": limit,
        }

    except HTTPException as he:
        # 🔥 ULTRA! Properly propagate HTTP exceptions
        logger.error(f"HTTP exception in list_ontologies: {he.status_code} - {he.detail}")
        raise he
    except Exception as e:
        logger.error(f"Failed to list ontologies: {e}")
        # 🔥 ULTRA! Check if it's a 404 from OMS
        if "404" in str(e) or "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다"
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 목록 조회 실패: {str(e)}",
        )


@router.get("/ontology/{class_label}", response_model=OntologyResponse)
async def get_ontology(
    db_name: str,
    class_label: str,
    request: Request,
    mapper: LabelMapper = Depends(get_label_mapper),
    terminus: TerminusService = Depends(get_terminus_service),
):
    """
    온톨로지 조회

    레이블 또는 ID로 온톨로지를 조회합니다.
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        # URL 파라미터 디버깅
        logger.info(
            f"Getting ontology - db_name: {db_name}, class_label: {class_label}, lang: {lang}"
        )

        # 레이블로 ID 조회 시도
        class_id = await mapper.get_class_id(db_name, class_label, lang)
        logger.info(f"Mapped label '{class_label}' to class_id: {class_id}")

        # ID가 없으면 입력값을 ID로 간주
        if not class_id:
            class_id = class_label
            logger.info(f"No mapping found, using label as ID: {class_id}")

        # 온톨로지 조회
        result = await terminus.get_class(db_name, class_id)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_label}'을(를) 찾을 수 없습니다",
            )

        logger.info(f"Raw result from terminus.get_class: {result}")

        # Extract ontology data (terminus.get_class already extracts from data field)
        ontology_data = result if isinstance(result, dict) else {}

        # 레이블 정보 추가
        ontology_data = await mapper.convert_to_display(db_name, ontology_data, lang)
        logger.info(f"After convert_to_display: {ontology_data}")

        # Ensure result has required fields for OntologyResponse
        if not ontology_data.get("id"):
            ontology_data["id"] = class_id
        if not ontology_data.get("label"):
            # Try to get label from the mapper
            label = await mapper.get_class_label(db_name, class_id, lang)
            ontology_data["label"] = label or class_label

        # Remove extra fields that are not part of OntologyBase
        # Ensure label and description are simple strings
        label_value = ontology_data.get("label")
        if isinstance(label_value, dict):
            # Extract string from dict (fallback to English if available)
            label_value = label_value.get(lang) or label_value.get("en") or label_value.get("ko") or class_label
        elif not isinstance(label_value, str):
            label_value = str(label_value) if label_value else class_label
        
        desc_value = ontology_data.get("description")
        if isinstance(desc_value, dict):
            # Extract string from dict (fallback to English if available)
            desc_value = desc_value.get(lang) or desc_value.get("en") or desc_value.get("ko") or ""
        elif not isinstance(desc_value, str) and desc_value:
            desc_value = str(desc_value)
        
        ontology_base_data = {
            "id": ontology_data.get("id"),
            "label": label_value,
            "description": desc_value,
            "properties": ontology_data.get("properties", []),
            "relationships": ontology_data.get("relationships", []),
            "parent_class": ontology_data.get("parent_class"),
            "abstract": ontology_data.get("abstract", False),
            "metadata": ontology_data.get("metadata", {}),
        }

        # OntologyResponse inherits from OntologyBase, so pass fields directly
        return OntologyResponse(**ontology_base_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 조회 실패: {str(e)}",
        )


@router.put("/ontology/{class_label}")
async def update_ontology(
    db_name: str,
    class_label: str,
    ontology: OntologyUpdateInput,
    request: Request,
    mapper: LabelMapper = Depends(get_label_mapper),
    terminus: TerminusService = Depends(get_terminus_service),
):
    """
    온톨로지 수정

    기존 온톨로지를 수정합니다.
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        # 레이블로 ID 조회
        class_id = await mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = class_label

        # 업데이트 데이터 준비
        update_data = ontology.dict(exclude_unset=True)
        update_data["id"] = class_id

        # 온톨로지 업데이트
        await terminus.update_class(db_name, class_id, update_data)

        # 레이블 매핑 업데이트
        await mapper.update_mappings(db_name, update_data)

        return {
            "message": f"온톨로지 '{class_label}'이(가) 수정되었습니다",
            "id": class_id,
            "updated_fields": list(update_data.keys()),
        }

    except HTTPException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"온톨로지 '{e.ontology_id}'을(를) 찾을 수 없습니다",
        )
    except HTTPException as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"유효성 검증 실패: {e.message}",
        )
    except Exception as e:
        logger.error(f"Failed to update ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 수정 실패: {str(e)}",
        )


@router.delete("/ontology/{class_label}")
async def delete_ontology(
    db_name: str,
    class_label: str,
    request: Request,
    mapper: LabelMapper = Depends(get_label_mapper),
    terminus: TerminusService = Depends(get_terminus_service),
):
    """
    온톨로지 삭제

    온톨로지를 삭제합니다.
    주의: 이 작업은 되돌릴 수 없습니다!
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        # 레이블로 ID 조회
        class_id = await mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = class_label

        # 온톨로지 삭제
        await terminus.delete_class(db_name, class_id)

        # 레이블 매핑 삭제
        await mapper.remove_class(db_name, class_id)

        return {"message": f"온톨로지 '{class_label}'이(가) 삭제되었습니다", "id": class_id}

    except HTTPException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"온톨로지 '{e.ontology_id}'을(를) 찾을 수 없습니다",
        )
    except Exception as e:
        logger.error(f"Failed to delete ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 삭제 실패: {str(e)}",
        )


@router.get("/ontology/{class_id}/schema")
async def get_ontology_schema(
    db_name: str,
    class_id: str,
    request: Request,
    format: str = Query("json", description="스키마 형식 (json, jsonld, owl)"),
    mapper: LabelMapper = Depends(get_label_mapper),
    terminus: TerminusService = Depends(get_terminus_service),
    jsonld_conv: JSONToJSONLDConverter = Depends(get_jsonld_converter),
):
    """
    온톨로지 스키마 조회

    온톨로지의 스키마를 다양한 형식으로 조회합니다.
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = sanitize_input(class_id)
        # 레이블로 ID 조회
        actual_id = await mapper.get_class_id(db_name, class_id, lang)
        if not actual_id:
            actual_id = class_id

        # 온톨로지 조회
        ontology = await terminus.get_class(db_name, actual_id)

        if not ontology:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'을(를) 찾을 수 없습니다",
            )

        # 형식별 변환
        if format == "jsonld":
            schema = jsonld_conv.convert_to_jsonld(ontology, db_name)
        elif format == "owl":
            # OWL 형식은 JSON-LD로 대체 (RDF 호환)
            schema = jsonld_conv.convert_to_jsonld(ontology, db_name)
            # OWL 형식 표시를 위한 메타데이터 추가
            if isinstance(schema, dict):
                schema["@comment"] = "This is a JSON-LD representation compatible with OWL"
                schema["@owl:versionInfo"] = "1.0"
        else:
            # 기본 JSON
            schema = ontology

        return schema

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get ontology schema: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 스키마 조회 실패: {str(e)}",
        )


# 🔥 THINK ULTRA! BFF Enhanced Relationship Management Endpoints


@router.post("/ontology-advanced", response_model=OntologyResponse)
async def create_ontology_with_relationship_validation(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    adapter: BFFAdapterService = Depends(get_bff_adapter),
):
    """
    🔥 고급 관계 검증을 포함한 온톨로지 생성 (BFF 레이어 - 리팩토링됨)

    이제 중복 로직 없이 BFF Adapter를 통해 OMS로 모든 비즈니스 로직을 위임합니다.
    
    Features:
    - 레이블 기반 자동 ID 생성
    - 자동 역관계 생성
    - 관계 검증 및 무결성 체크
    - 순환 참조 탐지
    - 다국어 레이블 매핑
    """
    try:
        # BFF Adapter를 통해 모든 로직 위임 (중복 제거)
        return await adapter.create_advanced_ontology(
            db_name=db_name,
            ontology_data=ontology.dict(exclude_unset=True),
            language="ko"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ontology with advanced relationships: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"고급 온톨로지 생성 실패: {str(e)}",
        )


@router.post("/validate-relationships")
async def validate_ontology_relationships_bff(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    adapter: BFFAdapterService = Depends(get_bff_adapter),
):
    """
    🔥 온톨로지 관계 검증 (BFF 레이어 - 리팩토링됨)

    이제 중복 로직 없이 BFF Adapter를 통해 OMS로 모든 비즈니스 로직을 위임합니다.
    """
    try:
        # BFF Adapter를 통해 모든 로직 위임 (중복 제거)
        return await adapter.validate_relationships(
            db_name=db_name,
            validation_data=ontology.dict(exclude_unset=True),
            language="ko"
        )

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Failed to validate relationships: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"관계 검증 실패: {str(e)}"
        )


@router.post("/check-circular-references")
async def check_circular_references_bff(
    db_name: str,
    ontology: Optional[OntologyCreateRequestBFF] = None,
    adapter: BFFAdapterService = Depends(get_bff_adapter),
):
    """
    🔥 순환 참조 탐지 (BFF 레이어 - 리팩토링됨)

    이제 중복 로직 없이 BFF Adapter를 통해 OMS로 모든 비즈니스 로직을 위임합니다.
    """
    try:
        # 새 온톨로지 데이터 준비
        detection_data = {}
        if ontology:
            detection_data = ontology.dict(exclude_unset=True)

        # BFF Adapter를 통해 모든 로직 위임 (중복 제거)
        return await adapter.detect_circular_references(
            db_name=db_name,
            detection_data=detection_data,
            language="ko"
        )

    except Exception as e:
        logger.error(f"Failed to check circular references: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"순환 참조 탐지 실패: {str(e)}",
        )


@router.get("/relationship-network/analyze")
async def analyze_relationship_network_bff(
    db_name: str,
    request: Request,
    terminus: TerminusService = Depends(get_terminus_service),
    mapper: LabelMapper = Depends(get_label_mapper),
):
    """
    🔥 관계 네트워크 분석 (BFF 레이어)

    전체 관계 네트워크의 건강성을 분석하고 사용자 친화적 결과 반환
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        # 네트워크 분석 수행
        analysis_result = await terminus.analyze_relationship_network(db_name)

        # 사용자 친화적 형태로 변환
        summary = analysis_result.get("relationship_summary", {})
        validation = analysis_result.get("validation_summary", {})
        cycles = analysis_result.get("cycle_analysis", {})
        graph = analysis_result.get("graph_summary", {})
        recommendations = analysis_result.get("recommendations", [])

        # 건강성 점수 계산 (0-100)
        health_score = 100
        if validation.get("errors", 0) > 0:
            health_score -= validation["errors"] * 10
        if cycles.get("critical_cycles", 0) > 0:
            health_score -= cycles["critical_cycles"] * 15
        if validation.get("warnings", 0) > 0:
            health_score -= validation["warnings"] * 2
        health_score = max(0, min(100, health_score))

        # 건강성 등급 결정
        if health_score >= 90:
            health_grade = "Excellent"
            health_color = "green"
        elif health_score >= 70:
            health_grade = "Good"
            health_color = "blue"
        elif health_score >= 50:
            health_grade = "Fair"
            health_color = "yellow"
        else:
            health_grade = "Poor"
            health_color = "red"

        return {
            "status": "success",
            "message": "관계 네트워크 분석이 완료되었습니다",
            "network_health": {
                "score": health_score,
                "grade": health_grade,
                "color": health_color,
                "description": f"네트워크 건강성이 {health_grade.lower()} 상태입니다",
            },
            "statistics": {
                "total_ontologies": analysis_result.get("ontology_count", 0),
                "total_relationships": summary.get("total_relationships", 0),
                "relationship_types": len(graph.get("relationship_types", [])),
                "total_entities": graph.get("total_entities", 0),
                "average_connections": round(graph.get("average_connections_per_entity", 0), 2),
            },
            "quality_metrics": {
                "validation_errors": validation.get("errors", 0),
                "validation_warnings": validation.get("warnings", 0),
                "critical_cycles": cycles.get("critical_cycles", 0),
                "total_cycles": cycles.get("total_cycles", 0),
                "inverse_coverage": summary.get("inverse_coverage", "0/0 (0%)"),
            },
            "recommendations": [
                {
                    "priority": "high" if "❌" in rec else "medium" if "⚠️" in rec else "low",
                    "message": rec.replace("❌ ", "")
                    .replace("⚠️ ", "")
                    .replace("📝 ", "")
                    .replace("🔄 ", ""),
                }
                for rec in recommendations
            ],
            "metadata": {
                "analysis_timestamp": analysis_result.get("analysis_timestamp"),
                "database": db_name,
                "language": lang,
            },
        }

    except Exception as e:
        logger.error(f"Failed to analyze relationship network: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"관계 네트워크 분석 실패: {str(e)}",
        )


@router.get("/relationship-paths")
async def find_relationship_paths_bff(
    request: Request,
    db_name: str,
    start_entity: str,
    end_entity: Optional[str] = Query(
        None, description="목표 엔티티 (없으면 모든 도달 가능한 엔티티)"
    ),
    max_depth: int = Query(3, ge=1, le=5, description="최대 탐색 깊이"),
    path_type: str = Query("shortest", description="경로 타입 (shortest, all, weighted, semantic)"),
    terminus: TerminusService = Depends(get_terminus_service),
    mapper: LabelMapper = Depends(get_label_mapper),
):
    """
    🔥 관계 경로 탐색 (BFF 레이어)

    엔티티 간의 관계 경로를 찾아 사용자 친화적 형태로 반환
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        start_entity = sanitize_input(start_entity)
        if end_entity:
            end_entity = sanitize_input(end_entity)
        # 레이블을 ID로 변환
        start_id = await mapper.get_class_id(db_name, start_entity, lang)
        if not start_id:
            start_id = start_entity

        end_id = None
        if end_entity:
            end_id = await mapper.get_class_id(db_name, end_entity, lang)
            if not end_id:
                end_id = end_entity

        # 경로 탐색 수행
        path_result = await terminus.find_relationship_paths(
            db_name=db_name,
            start_entity=start_id,
            end_entity=end_id,
            max_depth=max_depth,
            path_type=path_type,
        )

        # ID를 레이블로 변환하여 사용자 친화적 결과 생성
        paths = path_result.get("paths", [])
        user_friendly_paths = []

        for path in paths:
            # 엔티티 ID들을 레이블로 변환
            entity_labels = []
            for entity_id in path.get("entities", []):
                label = await mapper.get_class_label(db_name, entity_id, lang)
                entity_labels.append(label or entity_id)

            user_friendly_paths.append(
                {
                    "start": entity_labels[0] if entity_labels else start_entity,
                    "end": (
                        entity_labels[-1]
                        if len(entity_labels) > 1
                        else entity_labels[0] if entity_labels else ""
                    ),
                    "path": " → ".join(entity_labels),
                    "predicates": path.get("predicates", []),
                    "length": path.get("length", 0),
                    "weight": round(path.get("total_weight", 0), 2),
                    "confidence": path.get("confidence", 1.0),
                    "path_type": path.get("path_type", "unknown"),
                }
            )

        statistics = path_result.get("statistics", {})

        return {
            "status": "success",
            "message": f"관계 경로 탐색이 완료되었습니다 ({len(user_friendly_paths)}개 경로 발견)",
            "query": {
                "start_entity": start_entity,
                "end_entity": end_entity,
                "max_depth": max_depth,
                "path_type": path_type,
            },
            "paths": user_friendly_paths[:10],  # 최대 10개만 반환
            "statistics": {
                "total_paths_found": len(paths),
                "displayed_paths": min(len(user_friendly_paths), 10),
                "average_length": statistics.get("average_length", 0),
                "shortest_path_length": statistics.get("min_length", 0),
                "longest_path_length": statistics.get("max_length", 0),
            },
            "metadata": {"language": lang, "start_id": start_id, "end_id": end_id},
        }

    except Exception as e:
        logger.error(f"Failed to find relationship paths: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"관계 경로 탐색 실패: {str(e)}",
        )


@router.post("/suggest-schema-from-data")
async def suggest_schema_from_data(
    db_name: str,
    request: SchemaFromDataRequest,
    terminus: TerminusService = Depends(get_terminus_service),
):
    """
    🔥 데이터에서 스키마 자동 제안

    제공된 데이터를 분석하여 OMS 온톨로지 스키마를 자동으로 생성합니다.
    이는 Funnel 서비스의 핵심 기능을 통합한 엔드포인트입니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        from bff.services.funnel_client import FunnelClient

        # Funnel 클라이언트를 통해 스키마 제안 요청
        async with FunnelClient() as funnel_client:
            # 데이터 분석 및 스키마 제안을 한 번에 수행
            result = await funnel_client.analyze_and_suggest_schema(
                data=request.data,
                columns=request.columns,
                class_name=request.class_name,
                include_complex_types=request.include_complex_types,
            )

            analysis = result.get("analysis", {})
            schema_suggestion = result.get("schema_suggestion", {})

            # 분석 결과 요약
            analyzed_columns = analysis.get("columns", [])
            high_confidence_types = [
                col
                for col in analyzed_columns
                if col.get("inferred_type", {}).get("confidence", 0) >= 0.7
            ]

            return {
                "status": "success",
                "message": f"스키마 제안이 완료되었습니다. {len(high_confidence_types)}/{len(analyzed_columns)} 컬럼에 대해 높은 신뢰도 타입 추론 성공",
                "suggested_schema": schema_suggestion,
                "analysis_summary": {
                    "total_columns": len(analyzed_columns),
                    "high_confidence_columns": len(high_confidence_types),
                    "average_confidence": (
                        round(
                            sum(
                                col.get("inferred_type", {}).get("confidence", 0)
                                for col in analyzed_columns
                            )
                            / len(analyzed_columns),
                            2,
                        )
                        if analyzed_columns
                        else 0
                    ),
                    "detected_types": list(
                        set(
                            col.get("inferred_type", {}).get("type", "unknown")
                            for col in analyzed_columns
                        )
                    ),
                },
                "detailed_analysis": analysis,
            }

    except Exception as e:
        logger.error(f"Schema suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"스키마 제안 실패: {str(e)}"
        )


@router.post("/suggest-schema-from-google-sheets")
async def suggest_schema_from_google_sheets(
    db_name: str,
    request: SchemaFromGoogleSheetsRequest,
    terminus: TerminusService = Depends(get_terminus_service),
):
    """
    🔥 Google Sheets에서 스키마 자동 제안

    Google Sheets URL에서 데이터를 가져와 분석하고 OMS 온톨로지 스키마를 자동으로 생성합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        from bff.services.funnel_client import FunnelClient

        # Funnel 클라이언트를 통해 Google Sheets 스키마 제안 요청
        async with FunnelClient() as funnel_client:
            result = await funnel_client.google_sheets_to_schema(
                sheet_url=request.sheet_url,
                worksheet_name=request.worksheet_name,
                class_name=request.class_name,
                api_key=request.api_key,
            )

            preview = result.get("preview", {})
            schema_suggestion = result.get("schema_suggestion", {})

            # 미리보기 정보 요약
            total_rows = preview.get("total_rows", 0)
            preview_rows = preview.get("preview_rows", 0)
            columns = preview.get("columns", [])

            return {
                "status": "success",
                "message": f"Google Sheets 스키마 제안이 완료되었습니다. {total_rows}행 중 {preview_rows}행 분석됨",
                "suggested_schema": schema_suggestion,
                "source_info": {
                    "sheet_url": request.sheet_url,
                    "worksheet_name": request.worksheet_name,
                    "total_rows": total_rows,
                    "preview_rows": preview_rows,
                    "columns": columns,
                },
                "preview_data": preview,
            }

    except Exception as e:
        logger.error(f"Google Sheets schema suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets 스키마 제안 실패: {str(e)}",
        )
